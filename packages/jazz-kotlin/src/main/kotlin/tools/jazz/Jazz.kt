package tools.jazz

import kotlinx.coroutines.Dispatchers
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

/** What jazz-ffi's `JazzError::Closed` reads as in a subscription's error. */
internal const val CLOSED_MESSAGE: String = "the app is closed"

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
) : AutoCloseable {
    public companion object {
        /** How [open] reaches jazz-ffi; replace it to open through something else. */
        @Volatile
        public var opener: Native.Opener = Native.Opener { config -> openReflectively("tools.jazz.UniffiNative", config) }

        /** Opens the app [app] (the text of its `jazz.app.json`), as generated bindings' `open` does. */
        public suspend fun open(app: String, config: JazzConfig, implementations: Any? = null): Jazz = io {
            Jazz(opener.open(config.toJson(app)), implementations)
        }

        private suspend fun <T> io(block: () -> T): T = withContext(Dispatchers.IO) { block() }
    }

    /** The account id of the user this backend acts as, or `null` for a client. */
    public suspend fun account(): String? = io { native.account() }

    /** Every row [query] matches. */
    public suspend fun <Row : Any> all(query: Query<Row>): List<Row> = io {
        (jazzJson.parseToJsonElement(native.all(query.toJson())) as JsonArray).map { decode(query, it as JsonObject) }
    }

    /** The first row [query] matches, or `null`. */
    public suspend fun <Row : Any> one(query: Query<Row>): Row? = io {
        native.one(query.toJson())?.let { decode(query, jazzJson.parseToJsonElement(it) as JsonObject) }
    }

    /** Inserts [row] into [table]; returns the stored row with its `id`. */
    public suspend fun <Row : Any, Insert : Any, P : Patch> insert(table: Table<Row, Insert, P>, row: Insert): Row =
        io {
            val values = jazzJson.encodeToJsonElement(table.insertSerializer, row) as JsonObject
            val json = table.toStored(values, implementations).toString()
            val stored = jazzJson.parseToJsonElement(native.insert(table.tableName, json)) as JsonObject
            jazzJson.decodeFromJsonElement(table.rowSerializer, table.fromStored(stored, implementations))
        }

    /** Changes the row [id] of [table]: `jazz.update(Tasks, id) { status(TaskStatus.Done) }`. */
    public suspend fun <Row : Any, Insert : Any, P : Patch> update(
        table: Table<Row, Insert, P>,
        id: String,
        patch: P.() -> Unit,
    ): Unit = io {
        val p = table.newPatch().apply(patch)
        val values = jazzJson.parseToJsonElement(p.toJson()) as JsonObject
        native.update(table.tableName, id, table.toStored(values, implementations).toString())
    }

    /** Deletes the row [id] of [table]. */
    public suspend fun delete(table: Table<*, *, *>, id: String): Unit = io { native.delete(table.tableName, id) }

    /**
     * The deltas of [query]'s result as it changes, starting with a reset
     * holding the full result. Collecting subscribes; cancelling the
     * collection unsubscribes. Deltas carry no positions: use [watch] for
     * the ordered result.
     */
    public fun <Row : Any> subscribe(query: Query<Row>): Flow<Delta<Row>> = raw(query).map { d ->
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
    public fun <Row : Any> watch(query: Query<Row>): Flow<List<Row>> = flow {
        val rows = LinkedHashMap<String, JsonObject>()
        raw(query).collect { d ->
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

    private fun raw(query: Query<*>): Flow<RawDelta> = callbackFlow {
        // Kept as soon as the native core returns it: a cancel while
        // subscribing (withContext then throws) still unsubscribes.
        var cancel: (() -> Unit)? = null
        try {
            withContext(Dispatchers.IO + kotlinx.coroutines.NonCancellable) {
                cancel = native.subscribe(
                query.toJson(),
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
                onError = { message ->
                    // jazz-ffi ends every open subscription with its Closed error when the app closes.
                    close(if (message == CLOSED_MESSAGE) JazzException.Closed() else JazzException.Subscription(message))
                },
            )
            }
            awaitClose()
        } finally {
            cancel?.invoke()
        }
    }.buffer(Channel.UNLIMITED)

    private fun <Row : Any> decode(query: Query<Row>, row: JsonObject): Row =
        jazzJson.decodeFromJsonElement(query.rowSerializer, prepareRow(query.source, query.includes, row, implementations))

    /**
     * Closes the app: open subscriptions (and `watch`es) end with
     * [JazzException.Closed], and so do later calls. Closing again does
     * nothing, so `use {}` and an explicit `close()` mix.
     */
    override fun close(): Unit = native.close()

    private suspend fun <T> io(block: () -> T): T = withContext(Dispatchers.IO) { block() }
}
