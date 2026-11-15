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
public class Jazz internal constructor(private val native: Native) : AutoCloseable {
    public companion object {
        /** How [open] reaches jazz-ffi; replace it to open through something else. */
        @Volatile
        public var opener: Native.Opener = Native.Opener { config -> openReflectively("tools.jazz.UniffiNative", config) }

        /** Opens the app [app] (the text of its `jazz.app.json`), as generated bindings' `open` does. */
        public suspend fun open(app: String, config: JazzConfig): Jazz = io {
            Jazz(opener.open(config.toJson(app)))
        }

        private suspend fun <T> io(block: () -> T): T = withContext(Dispatchers.IO) { block() }
    }

    /** The account id of the user this backend acts as, or `null` for a client. */
    public suspend fun account(): String? = io { native.account() }

    /** Every row [query] matches. */
    public suspend fun <Row : Any> all(query: Query<Row>): List<Row> = io {
        jazzJson.decodeFromString(ListSerializer(query.rowSerializer), native.all(query.toJson()))
    }

    /** The first row [query] matches, or `null`. */
    public suspend fun <Row : Any> one(query: Query<Row>): Row? = io {
        native.one(query.toJson())?.let { jazzJson.decodeFromString(query.rowSerializer, it) }
    }

    /** Inserts [row] into [table]; returns the stored row with its `id`. */
    public suspend fun <Row : Any, Insert : Any, P : Patch> insert(table: Table<Row, Insert, P>, row: Insert): Row =
        io {
            val json = jazzJson.encodeToString(table.insertSerializer, row)
            jazzJson.decodeFromString(table.rowSerializer, native.insert(table.tableName, json))
        }

    /** Changes the row [id] of [table]: `jazz.update(Tasks, id) { status(TaskStatus.Done) }`. */
    public suspend fun <Row : Any, Insert : Any, P : Patch> update(
        table: Table<Row, Insert, P>,
        id: String,
        patch: P.() -> Unit,
    ): Unit = io {
        val p = table.newPatch().apply(patch)
        native.update(table.tableName, id, p.toJson())
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
            added = d.added.map { jazzJson.decodeFromJsonElement(query.rowSerializer, it) },
            updated = d.updated.map { jazzJson.decodeFromJsonElement(query.rowSerializer, it) },
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
            emit(query.sort(rows.values.toList()).map { jazzJson.decodeFromJsonElement(query.rowSerializer, it) })
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
        // Subscribed outside withContext's cancellation: a cancel while
        // subscribing still reaches `cancel` below.
        val cancel = withContext(Dispatchers.IO + kotlinx.coroutines.NonCancellable) {
            native.subscribe(
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
                onError = { message -> close(JazzException.Subscription(message)) },
            )
        }
        awaitClose { cancel() }
    }.buffer(Channel.UNLIMITED)

    /** Closes the app; later calls fail with [JazzException.Closed]. */
    override fun close(): Unit = native.close()

    private suspend fun <T> io(block: () -> T): T = withContext(Dispatchers.IO) { block() }
}
