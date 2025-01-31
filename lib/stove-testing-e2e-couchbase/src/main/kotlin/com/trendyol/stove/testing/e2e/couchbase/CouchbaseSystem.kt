package com.trendyol.stove.testing.e2e.couchbase

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.msg.kv.DurabilityLevel.PERSIST_TO_MAJORITY
import com.couchbase.client.java.*
import com.couchbase.client.java.codec.JacksonJsonSerializer
import com.couchbase.client.java.env.ClusterEnvironment
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.kv.InsertOptions
import com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS
import com.fasterxml.jackson.databind.ObjectMapper
import com.trendyol.stove.functional.Try
import com.trendyol.stove.functional.recover
import com.trendyol.stove.testing.e2e.couchbase.ClusterExtensions.executeQueryAs
import com.trendyol.stove.testing.e2e.system.TestSystem
import com.trendyol.stove.testing.e2e.system.abstractions.ExposesConfiguration
import com.trendyol.stove.testing.e2e.system.abstractions.PluggedSystem
import com.trendyol.stove.testing.e2e.system.abstractions.RunAware
import com.trendyol.stove.testing.e2e.system.abstractions.StateOfSystem
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class CouchbaseSystem internal constructor(
    override val testSystem: TestSystem,
    val context: CouchbaseContext
) : PluggedSystem, RunAware, ExposesConfiguration {
    @PublishedApi
    internal lateinit var cluster: ReactiveCluster

    @PublishedApi
    internal lateinit var collection: ReactiveCollection

    @PublishedApi
    internal val objectMapper: ObjectMapper = context.options.objectMapper

    private lateinit var exposedConfiguration: CouchbaseExposedConfiguration
    private val logger: Logger = LoggerFactory.getLogger(javaClass)
    private val state: StateOfSystem<CouchbaseSystem, CouchbaseExposedConfiguration> =
        StateOfSystem(
            testSystem.options,
            CouchbaseSystem::class,
            CouchbaseExposedConfiguration::class
        )

    override suspend fun run() {
        exposedConfiguration =
            state.capture {
                context.container.start()
                val couchbaseHostWithPort = context.container.connectionString.replace("couchbase://", "")
                CouchbaseExposedConfiguration(
                    context.container.connectionString,
                    couchbaseHostWithPort,
                    context.container.username,
                    context.container.password
                )
            }

        cluster = createCluster(exposedConfiguration)
        collection = cluster.bucket(context.bucket.name).defaultCollection()
        if (!state.isSubsequentRun()) {
            context.options.migrationCollection.run(cluster)
        }
    }

    override suspend fun stop(): Unit = context.container.stop()

    override fun configuration(): List<String> =
        context.options.configureExposedConfiguration(exposedConfiguration) +
            listOf(
                "couchbase.hosts=${exposedConfiguration.hostsWithPort}",
                "couchbase.username=${exposedConfiguration.username}",
                "couchbase.password=${exposedConfiguration.password}"
            )

    @Suppress("unused")
    suspend inline fun <reified T : Any> shouldQuery(
        query: String,
        assertion: (List<T>) -> Unit
    ): CouchbaseSystem {
        val result = cluster.executeQueryAs<Any>(query) { queryOptions -> queryOptions.scanConsistency(REQUEST_PLUS) }

        val objects =
            result
                .map { objectMapper.writeValueAsString(it) }
                .map { objectMapper.readValue(it, T::class.java) }

        assertion(objects)
        return this
    }

    suspend inline fun <reified T : Any> shouldGet(
        key: String,
        assertion: (T) -> Unit
    ): CouchbaseSystem =
        collection.get(key)
            .awaitSingle().contentAs(T::class.java)
            .let(assertion)
            .let { this }

    suspend inline fun <reified T : Any> shouldGet(
        collection: String,
        key: String,
        assertion: (T) -> Unit
    ): CouchbaseSystem =
        cluster.bucket(context.bucket.name)
            .collection(collection)
            .get(key).awaitSingle()
            .contentAs(T::class.java)
            .let(assertion)
            .let { this }

    suspend fun shouldNotExist(key: String): CouchbaseSystem =
        when (
            collection.get(key)
                .onErrorResume { throwable ->
                    when (throwable) {
                        is DocumentNotFoundException -> Mono.empty()
                        else -> throw throwable
                    }
                }.awaitFirstOrNull()
        ) {
            null -> this
            else -> throw AssertionError("The document with the given id($key) was not expected, but found!")
        }

    @Suppress("unused")
    suspend fun shouldNotExist(
        collection: String,
        key: String
    ): CouchbaseSystem =
        when (
            cluster
                .bucket(context.bucket.name)
                .collection(collection)
                .get(key)
                .onErrorResume { throwable ->
                    when (throwable) {
                        is DocumentNotFoundException -> Mono.empty()
                        else -> throw throwable
                    }
                }.awaitFirstOrNull()
        ) {
            null -> this
            else -> throw AssertionError("The document with the given id($key) was not expected, but found!")
        }

    @Suppress("unused")
    suspend fun shouldDelete(key: String): CouchbaseSystem =
        collection.remove(key).awaitSingle()
            .let { this }

    @Suppress("unused")
    suspend fun shouldDelete(
        collection: String,
        key: String
    ): CouchbaseSystem =
        cluster.bucket(context.bucket.name)
            .collection(collection)
            .remove(key)
            .awaitSingle().let { this }

    /**
     * Saves the [instance] with given [id] to the [collection]
     * To save to the default collection use [saveToDefaultCollection]
     */
    suspend fun <T : Any> save(
        collection: String,
        id: String,
        instance: T
    ): CouchbaseSystem =
        cluster
            .bucket(context.bucket.name)
            .collection(collection)
            .insert(
                id,
                JsonObject.fromJson(objectMapper.writeValueAsString(instance)),
                InsertOptions.insertOptions().durability(PERSIST_TO_MAJORITY)
            )
            .awaitSingle().let { this }

    /**
     * Saves the [instance] with given [id] to the default collection
     * In couchbase the default collection is `_default`
     */
    suspend inline fun <reified T : Any> saveToDefaultCollection(
        id: String,
        instance: T
    ): CouchbaseSystem = this.save("_default", id, instance)

    override fun close(): Unit =
        runBlocking {
            Try {
                cluster.disconnect().awaitSingle()
                executeWithReuseCheck { stop() }
            }.recover {
                logger.warn("Disconnecting the couchbase cluster got an error: $it")
            }
        }

    private fun createCluster(exposedConfiguration: CouchbaseExposedConfiguration): ReactiveCluster =
        ClusterEnvironment.builder()
            .jsonSerializer(JacksonJsonSerializer.create(objectMapper))
            .build()
            .let {
                Cluster.connect(
                    exposedConfiguration.hostsWithPort,
                    ClusterOptions
                        .clusterOptions(exposedConfiguration.username, exposedConfiguration.password)
                        .environment(it)
                ).reactive()
            }

    companion object {
        /**
         * Exposes the [ReactiveCluster] of the [CouchbaseSystem]
         */
        @Suppress("unused")
        fun CouchbaseSystem.cluster(): ReactiveCluster = this.cluster

        /**
         * Exposes the [ReactiveBucket] of the [CouchbaseSystem]
         */
        @Suppress("unused")
        fun CouchbaseSystem.bucket(): ReactiveBucket = this.cluster.bucket(this.context.bucket.name)
    }
}
