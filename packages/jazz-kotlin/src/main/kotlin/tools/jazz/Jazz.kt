package tools.jazz

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive

/**
 * `className.open(config)`, by reflection: UniffiNative is compiled from the
 * sources uniffi-bindgen generates for jazz-ffi (see build.gradle.kts), so
 * the rest of the library builds without them. What `open` throws is
 * rethrown as it is, not wrapped.
 */
internal fun openReflectively(className: String, config: String): Native {
    val native = Class.forName(className)
    return try {
        native.getMethod("open", String::class.java).invoke(null, config) as Native
    } catch (e: java.lang.reflect.InvocationTargetException) {
        throw e.targetException
    }
}

/** The JSON settings rows, inserts and patches go through. */
public val jazzJson: Json = Json {
    ignoreUnknownKeys = true
    encodeDefaults = false
    explicitNulls = false
}

/**
 * What changed in a subscription's result. The first delta is a [reset]
 * with the whole result in [added].
 */
public data class Delta<Row>(
    val added: List<Row>,
    val updated: List<Row>,
    val removed: List<String>,
    val reset: Boolean,
    val coalesced: Boolean,
)

/** An open app. Every call may be made from any coroutine; [close] when done. */
public class Jazz internal constructor(
    private val native: Native,
    /** The app's `JazzComputed`, for transforms and computed fields. */
    private val implementations: Any? = null,
    /** Where blocking native calls run ([JazzConfig.dispatcher]). */
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {
    public companion object {
        /** How [open] reaches jazz-ffi; replace it to open through something else. */
        @Volatile
        public var opener: Native.Opener = Native.Opener { config -> openReflectively("tools.jazz.UniffiNative", config) }

        /** Opens the app [app] (the text of its `jazz.app.json`), as generated bindings' `open` does. */
        public suspend fun open(app: String, config: JazzConfig, implementations: Any? = null): Jazz =
            withContext(config.dispatcher) { Jazz(opener.open(config.toJson(app)), implementations, config.dispatcher) }
    }

    /** The account id of the user this backend acts as, or `null` for a client. */
    public suspend fun account(): String? = io { native.account() }

    /** Every row [query] matches, read at [tier] (by default local-first). */
    public suspend fun <Row : Any> all(query: Query<Row>, tier: ReadTier? = null): List<Row> = io {
        val json = if (tier == null) native.all(query.toJson()) else native.allAt(query.toJson(), tier.wire)
        decodeAll(query, json)
    }

    /** The first row [query] matches, or `null`, read at [tier]. */
    public suspend fun <Row : Any> one(query: Query<Row>, tier: ReadTier? = null): Row? = io {
        val json = if (tier == null) native.one(query.toJson()) else native.oneAt(query.toJson(), tier.wire)
        json?.let { decode(query, jazzJson.parseToJsonElement(it) as JsonObject) }
    }

    /**
     * Inserts [row] into [table]; returns the stored row with its `id`. With
     * [tier] [WriteTier.Global] it returns once the sync server accepted it,
     * and a refusal throws ([JazzException.Permission]).
     */
    public suspend fun <Row : Any, Insert : Any, P : Patch> insert(
        table: Table<Row, Insert, P>,
        row: Insert,
        tier: WriteTier? = null,
    ): Row = io {
        val json = insertJson(table, row)
        val stored = if (tier == null) native.insert(table.tableName, json) else native.insertAt(table.tableName, json, tier.wire)
        decodeStored(table, stored)
    }

    /** Changes the row [id] of [table]: `jazz.update(Tasks, id) { status(TaskStatus.Done) }`. */
    public suspend fun <Row : Any, Insert : Any, P : Patch> update(
        table: Table<Row, Insert, P>,
        id: String,
        tier: WriteTier? = null,
        patch: P.() -> Unit,
    ): Unit = io {
        val json = patchJson(table, patch)
        if (tier == null) native.update(table.tableName, id, json) else native.updateAt(table.tableName, id, json, tier.wire)
    }

    /** Deletes the row [id] of [table]. */
    public suspend fun delete(table: Table<*, *, *>, id: String, tier: WriteTier? = null): Unit = io {
        if (tier == null) native.delete(table.tableName, id) else native.deleteAt(table.tableName, id, tier.wire)
    }

    /** Returns once every write made so far reached [tier]; throws if the server refused one. */
    public suspend fun waitForSync(tier: WriteTier = WriteTier.Global): Unit = io { native.waitForSync(tier.wire) }

    /**
     * Runs [block] in a transaction: its reads see its own writes, and its
     * writes apply together. It commits when [block] returns (at [tier]:
     * with [WriteTier.Global] it returns once the server accepted it) and
     * rolls back when [block] throws, rethrowing. Returns [block]'s value.
     *
     * ```kotlin
     * val moved = jazz.transaction { tx ->
     *     val task = tx.one(Tasks.where(Tasks.id eq id)) ?: return@transaction false
     *     tx.update(Tasks, task.id) { status(TaskStatus.Done) }
     *     true
     * }
     * ```
     */
    public suspend fun <T> transaction(tier: WriteTier? = null, block: suspend (Transaction) -> T): T {
        val native = io { native.begin() }
        val tx = Transaction(native)
        val result = try {
            block(tx)
        } catch (e: Throwable) {
            withContext(NonCancellable) { tx.end { runCatching { native.rollback() } } }
            throw e
        }
        withContext(NonCancellable) { tx.end { native.commit(tier?.wire) } }
        return result
    }

    /**
     * A transaction [transaction] hands its block. Calls are serialized (the
     * native transaction takes one thread at a time); after the block it
     * throws [JazzException.Closed].
     */
    public inner class Transaction internal constructor(private val tx: NativeTransaction) {
        private val lock = Mutex()
        private var ended = false

        private suspend fun <T> run(block: () -> T): T = lock.withLock {
            if (ended) throw JazzException.Closed()
            io(block)
        }

        internal suspend fun end(block: () -> Unit): Unit = run {
            ended = true
            block()
        }

        public suspend fun <Row : Any> all(query: Query<Row>): List<Row> = run { decodeAll(query, tx.all(query.toJson())) }

        public suspend fun <Row : Any> one(query: Query<Row>): Row? =
            run { tx.one(query.toJson())?.let { decode(query, jazzJson.parseToJsonElement(it) as JsonObject) } }

        public suspend fun <Row : Any, Insert : Any, P : Patch> insert(table: Table<Row, Insert, P>, row: Insert): Row =
            run { decodeStored(table, tx.insert(table.tableName, insertJson(table, row))) }

        public suspend fun <Row : Any, Insert : Any, P : Patch> update(
            table: Table<Row, Insert, P>,
            id: String,
            patch: P.() -> Unit,
        ): Unit = run { tx.update(table.tableName, id, patchJson(table, patch)) }

        public suspend fun delete(table: Table<*, *, *>, id: String): Unit = run { tx.delete(table.tableName, id) }
    }

    private fun <Row : Any> decodeAll(query: Query<Row>, json: String): List<Row> =
        (jazzJson.parseToJsonElement(json) as JsonArray).map { decode(query, it as JsonObject) }

    private fun <Row : Any, Insert : Any> insertJson(table: Table<Row, Insert, *>, row: Insert): String {
        val values = jazzJson.encodeToJsonElement(table.insertSerializer, row) as JsonObject
        return table.toStored(values, implementations).toString()
    }

    private fun <Row : Any> decodeStored(table: Table<Row, *, *>, json: String): Row {
        val stored = jazzJson.parseToJsonElement(json) as JsonObject
        return jazzJson.decodeFromJsonElement(table.rowSerializer, table.fromStored(stored, implementations))
    }

    private fun <P : Patch> patchJson(table: Table<*, *, P>, patch: P.() -> Unit): String {
        val values = jazzJson.parseToJsonElement(table.newPatch().apply(patch).toJson()) as JsonObject
        return table.toStored(values, implementations).toString()
    }

    /**
     * The deltas of [query]'s result as it changes, starting with a reset
     * holding the full result. Collecting subscribes; cancelling the
     * collection unsubscribes. Deltas carry no positions: use [watch] for
     * the ordered result.
     */
    public fun <Row : Any> subscribe(query: Query<Row>, tier: ReadTier? = null): Flow<Delta<Row>> = raw(query, tier).map { d ->
        Delta(
            added = d.added.map { decode(query, it) },
            updated = d.updated.map { decode(query, it) },
            removed = d.removed,
            reset = d.reset,
            coalesced = d.coalesced,
        )
    }

    /**
     * [query]'s whole result each time it changes, ordered as its `orderBy`
     * says (re-sorted here: deltas carry no positions). `limit`/`offset`
     * apply to the server's result, not to the re-sorted one.
     */
    public fun <Row : Any> watch(query: Query<Row>, tier: ReadTier? = null): Flow<List<Row>> = flow {
        val rows = LinkedHashMap<String, JsonObject>()
        raw(query, tier).collect { d ->
            if (d.reset) rows.clear()
            for (id in d.removed) rows.remove(id)
            for (row in d.added + d.updated) rows[(row["id"] as JsonPrimitive).content] = row
            emit(query.sort(rows.values.toList()).map { decode(query, it) })
        }
    }.distinctUntilChanged()

    private class RawDelta(
        val added: List<JsonObject>,
        val updated: List<JsonObject>,
        val removed: List<String>,
        val reset: Boolean,
        val coalesced: Boolean,
    )

    private fun raw(query: Query<*>, tier: ReadTier?): Flow<RawDelta> = callbackFlow {
        // Kept as soon as the native core returns it: a cancel while
        // subscribing (withContext then throws) still unsubscribes.
        var cancel: (() -> Unit)? = null
        try {
            withContext(dispatcher + NonCancellable) {
                cancel = subscribeNative(
                query.toJson(),
                tier,
                onDelta = { json ->
                    val o = jazzJson.parseToJsonElement(json) as JsonObject
                    // Never dropped: a lost delta would leave `watch`'s result wrong for good.
                    trySendBlocking(
                        RawDelta(
                            added = (o["added"] as? JsonArray)?.objects() ?: emptyList(),
                            updated = (o["updated"] as? JsonArray)?.objects() ?: emptyList(),
                            removed = o["removed"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList(),
                            reset = o["reset"]?.jsonPrimitive?.booleanOrNull ?: false,
                            coalesced = o["coalesced"]?.jsonPrimitive?.booleanOrNull ?: false,
                        ),
                    )
                },
                // jazz-ffi ends every open subscription with one last error, by kind
                // (`closed` when the app closes).
                onError = { kind, message -> close(JazzException.of(kind, message)) },
            )
            }
            awaitClose()
        } finally {
            cancel?.invoke()
        }
    }.buffer(Channel.UNLIMITED)

    private fun subscribeNative(
        query: String,
        tier: ReadTier?,
        onDelta: (String) -> Unit,
        onError: (kind: String, message: String) -> Unit,
    ): () -> Unit =
        if (tier == null) native.subscribe(query, onDelta, onError) else native.subscribeAt(query, tier.wire, onDelta, onError)

    private fun <Row : Any> decode(query: Query<Row>, row: JsonObject): Row =
        jazzJson.decodeFromJsonElement(query.rowSerializer, prepareRow(query.source, query.includes, row, implementations))

    /**
     * Closes the app: open subscriptions (and `watch`es) end with
     * [JazzException.Closed], and so do later calls. Closing again does
     * nothing, so `use {}` and an explicit `close()` mix.
     */
    override fun close(): Unit = native.close()

    private suspend fun <T> io(block: () -> T): T = withContext(dispatcher) { block() }
}
