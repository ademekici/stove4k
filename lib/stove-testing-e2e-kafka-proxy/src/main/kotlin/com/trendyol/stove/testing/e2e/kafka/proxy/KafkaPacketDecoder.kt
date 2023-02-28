package com.trendyol.stove.testing.e2e.kafka.proxy

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder

class KafkaPacketDecoder : ByteToMessageDecoder() {
    override fun decode(
        ctx: ChannelHandlerContext,
        `in`: ByteBuf,
        out: MutableList<Any>,
    ) {
        if (`in`.readableBytes() < 4) {
            return
        }

        `in`.markReaderIndex()
        val size = `in`.readInt()
        if (`in`.readableBytes() < size - 4) {
            `in`.resetReaderIndex()
            return
        }

        val packet = `in`.readBytes(size - 4)
        out.add(packet)
    }
}
