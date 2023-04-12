package com.trendyol.stove.testing.e2e.kafka.proxy

import com.trendyol.stove.functional.Try
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.ResponseHeader
import org.slf4j.Logger

class ProxyServer(
    private val onPort: Int = 8080,
    private val targetHost: String,
    private val targetPort: Int,
) {

    private val logger: Logger = org.slf4j.LoggerFactory.getLogger(javaClass)
    fun start() {
        val serverSocket = ServerSocket(onPort)
        while (true) {
            val clientSocket = serverSocket.accept()
            val targetSocket = Socket(targetHost, targetPort)
            forward(clientSocket, targetSocket)
        }
    }

    private fun forward(
        clientSocket: Socket,
        targetSocket: Socket,
    ) {
        val clientToTargetThread = Thread {
            clientSocket.getInputStream().forwardTo2(targetSocket.getOutputStream())
        }

        val targetToClientThread = Thread {
            targetSocket.getInputStream().forwardTo2(clientSocket.getOutputStream())
        }

        clientToTargetThread.start()
        targetToClientThread.start()
    }

    private fun InputStream.forwardTo(out: OutputStream) {
        this.use { input ->
            out.use { output ->
                input.copyTo(output)
            }
        }
    }

    private fun InputStream.forwardTo2(out: OutputStream) {
        val byteBuffer = ByteArray(1024)
        var bytesRead: Int

        while (this.read(byteBuffer).also { bytesRead = it } != -1) {
            // Deserialize the request and response headers
            val requestHeader = deserializeHeader(byteBuffer, true)
            val responseHeader = deserializeHeader(byteBuffer, false)

            if (requestHeader != null) {
                when (requestHeader.apiKey()) {
                    ApiKeys.PRODUCE -> {
                        val produceRequest = deserializeRequest(byteBuffer, requestHeader)
                        if (produceRequest is ProduceRequest) {
                            println(produceRequest)
                            // produceRequest.part().forEach { (topicPartition, records) ->
                            //     records.forEach { record ->
                            //         // Extract information from the record
                            //         val key = record.key()
                            //         val value = record.value()
                            //         println(
                            //             "Produced message - Topic: ${topicPartition.topic()}, Partition: ${topicPartition.partition()}, Key: $key, Value: $value"
                            //         )
                            //     }
                            // }
                        }
                    }
                    // Handle other request types if needed
                    else -> {}
                }
            }

            if (responseHeader != null) {
                when (responseHeader.apiKey()) {
                    ApiKeys.PRODUCE -> {
                        val produceResponse = deserializeResponse(byteBuffer, responseHeader)
                        if (produceResponse is ProduceResponse) {
                            produceResponse.data().responses().map {
                                it.partitionResponses()
                            }.forEach { (topicPartition, partitionResponse) ->
                                // Extract information from the partition response
                                val errorCode = partitionResponse.errorCode()
                                val baseOffset = partitionResponse.baseOffset()
                                println(
                                    "Produce response - Topic: $topicPartition," +
                                        " Partition: $topicPartition, Error code: $errorCode, Base offset: $baseOffset"
                                )
                            }
                        }
                    }
                    // Handle other response types if needed
                    else -> {}
                }
            }

            out.write(byteBuffer, 0, bytesRead)
        }
    }

    private fun deserializeResponse(
        buffer: ByteArray,
        responseHeader: RequestHeader,
    ): AbstractResponse {
        val readable = ByteBuffer.wrap(buffer)
        return AbstractResponse.parseResponse(responseHeader.apiKey(), readable, API_VERSIONS.latestVersion())
    }

    private fun deserializeHeader(
        buffer: ByteArray,
        isRequest: Boolean,
    ): RequestHeader? {
        return try {
            val readable = ByteBuffer.wrap(buffer)
            val header = if (isRequest) RequestHeader.parse(readable) else ResponseHeader.parse(readable, API_VERSIONS.latestVersion())
            header as? RequestHeader
        } catch (e: Exception) {
            null
        }
    }

    private fun deserializeRequest(
        buffer: ByteArray,
        header: RequestHeader,
    ): AbstractRequest? {
        return try {
            val readable = ByteBuffer.wrap(buffer)
            // header.data().api.api.requestSchema.headerVersion().read(readable, ObjectSerializationCache())
            // header.data().api.requestSchema.parse(readable)
            AbstractRequest.parseRequest(header.apiKey(), API_VERSIONS.latestVersion(), readable).request
        } catch (e: Exception) {
            null
        }
    }

    // ###

    fun startProxy1() {
        val upstreamAddress = InetSocketAddress(targetHost, targetPort)
        ServerBootstrap()
            .group(NioEventLoopGroup(), NioEventLoopGroup())
            .channel(NioServerSocketChannel::class.java)
            .childHandler(object : ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                override fun initChannel(ch: io.netty.channel.socket.SocketChannel) {
                    ch.pipeline().addLast(KafkaPacketDecoder(), KafkaPacketForwarder(upstreamAddress))
                }
            })
            .bind(InetSocketAddress("localhost", onPort)).sync()
    }

    fun startProxy2() {
        val selector = Selector.open()
        val serverChannel = ServerSocketChannel.open()
        serverChannel.bind(InetSocketAddress("localhost", onPort))
        serverChannel.configureBlocking(false)
        serverChannel.register(selector, SelectionKey.OP_ACCEPT)

        while (true) {
            selector.select()

            val selectedKeys = selector.selectedKeys()
            val keyIterator = selectedKeys.iterator()

            while (keyIterator.hasNext()) {
                val key = keyIterator.next()

                if (key.isAcceptable) {
                    val serverSocketChannel = key.channel() as ServerSocketChannel
                    val clientChannel = serverSocketChannel.accept()
                    clientChannel.configureBlocking(false)

                    val remoteChannel = SocketChannel.open()
                    remoteChannel.connect(InetSocketAddress(targetHost, targetPort))
                    remoteChannel.configureBlocking(false)

                    clientChannel.register(selector, SelectionKey.OP_READ, remoteChannel)
                    remoteChannel.register(selector, SelectionKey.OP_READ, clientChannel)
                } else if (key.isReadable) {
                    val sourceChannel = key.channel() as SocketChannel
                    val destinationChannel = key.attachment() as SocketChannel

                    val buffer = ByteBuffer.allocate(1024)
                    val bytesRead = sourceChannel.read(buffer)

                    if (bytesRead == -1) {
                        sourceChannel.close()
                        destinationChannel.close()
                    } else {
                        buffer.flip()
                        val bytes = ByteArray(buffer.remaining())
                        buffer.get(bytes)

                        // Forward the data to the other socket
                        val outputBuffer = ByteBuffer.wrap(bytes)
                        ApiKeys.values()
                            .map {
                                Try { AbstractRequest.parseRequest(it, API_VERSIONS.latestVersion(), outputBuffer.asReadOnlyBuffer()) }
                                    .map {
                                        logger.info(it.request.toString(true))
                                    }
                            }
                        destinationChannel.write(outputBuffer)
                    }
                }

                keyIterator.remove()
            }
        }
    }
}
