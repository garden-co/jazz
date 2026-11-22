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
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.runBlocking
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
internal fun openReflectively(className: String, config: String, tokens: ((String) -> String?)? = null): Native {
    val native = Class.forName(className)
    return try {
        if (tokens == null) {
            native.getMethod("open", String::class.java).invoke(null, config) as Native
        } else {
            native.getMethod("openWithTokens", String::class.java, Function1::class.java).invoke(null, config, tokens) as Native
        }
    } catch (e: java.lang.reflect.InvocationTargetException) {
        throw e.targetException
    }
}

/**
 * What [start] returns on [dispatcher], for a handle someone must give back:
 * withContext drops its result when the caller was cancelled meanwhile, so
 * [giveBack] gets the handle then.
 */
private suspend fun <T : Any> startHandle(dispatcher: CoroutineDispatcher, giveBack: (T) -> Unit, start: () -> T): T {
    var started: T? = null
    try {
        return withContext(dispatcher) { start().also { started = it } }
    } catch (e: Throwable) {
        started?.let { handle -> withContext(dispatcher + NonCancellable) { runCatching { giveBack(handle) } } }
        throw e
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
        public var opener: Native.Opener = object : Native.Opener {
            override fun open(configJson: String): Native = openReflectively("tools.jazz.UniffiNative", configJson)

            override fun openWithTokens(configJson: String, tokens: (String) -> String?): Native =
                openReflectively("tools.jazz.UniffiNative", configJson, tokens)
        }

        /** Opens the app [app] (the text of its `jazz.app.json`), as generated bindings' `open` does. */
        public suspend fun open(app: String, config: JazzConfig, implementations: Any? = null): Jazz =
            startHandle(config.dispatcher, giveBack = { it.close() }) {
                if (config.serverUrl != null && config.auth == null) {
                    throw JazzException.Refused(
                        "connecting to ${config.serverUrl} needs JazzConfig.auth (a device account, or your backend's sign-in). " +
                            "Kotlin is for apps; your app's main backend runs in TypeScript.",
                    )
                }
                val json = config.toJson(app)
                val native = when (val auth = config.auth) {
                    // The native core asks on a thread of its own: blocking there is fine.
                    is JazzAuth.Bearer -> opener.openWithTokens(json) { reason ->
                        runBlocking { auth.token(TokenReason.of(reason)) }
                    }
                    else -> opener.open(json)
                }
                Jazz(native, implementations, config.dispatcher)
            }
    }

    /**
     * The end user's session: [SessionState.SignedIn] with who, `Expired`
     * or `SignedOut`, as it changes; `null` for a local app or a backend.
     */
    public val session: StateFlow<SessionState?> by lazy {
        stateFlow(sync = false, initial = native.sessionState()?.let(SessionState::parse)) { SessionState.parse(it) }
            .also { sessionFlow = it }
    }

    /** [session]'s flow once read, so [close] can end it as signed out. */
    @Volatile private var sessionFlow: MutableStateFlow<SessionState?>? = null

    /** How the app syncs ([SyncStatus.Connected], `Offline`, …), as it changes. */
    public val syncStatus: StateFlow<SyncStatus> by lazy { stateFlow(sync = true, initial = SyncStatus.parse(native.syncStatus())) { SyncStatus.parse(it) }.asStateFlow() }

    private val watches = java.util.concurrent.CopyOnWriteArrayList<() -> Unit>()

    private fun <T> stateFlow(sync: Boolean, initial: T, parse: (String) -> T): MutableStateFlow<T> {
        val flow = MutableStateFlow(initial)
        watches += native.watch(sync) { json ->
            // A closing app ends each watch with {"kind":"closed"}: not a state.
            if (!isClosedNotice(json)) flow.value = parse(json)
        }
        return flow
    }

    /** Stops syncing (TS's `disconnect`): reads and writes go on locally until [reconnect]. */
    public suspend fun disconnect(): Unit = io { native.disconnect() }

    /** Syncs again after [disconnect]. */
    public suspend fun reconnect(): Unit = io { native.reconnect() }

    /**
     * Closes the app and deletes its local store (e.g. on sign-out from a
     * shared device). Refused while another open app uses the same `dataDir`.
     */
    public suspend fun deleteClientStorage() {
        io { native.deleteClientStorage() }
        endWatches()
    }

    /**
     * Each write the server refused that nobody waited on (a local-first
     * write, not at [WriteTier.Global]), as TS's `onMutationError`.
     * Collecting starts watching.
     */
    public val mutationErrors: Flow<MutationError> get() = callbackFlow {
        val cancel = startHandle(dispatcher, giveBack = { it() }) {
            native.watchMutationErrors { json ->
                // The app closing ends the watch: the Flow completes.
                if (isClosedNotice(json)) close() else trySend(MutationError.parse(json))
            }
        }
        awaitClose { cancel() }
    }.buffer(Channel.UNLIMITED)

    private fun isClosedNotice(json: String): Boolean =
        json.contains("\"kind\"") &&
            ((jazzJson.parseToJsonElement(json) as? JsonObject)?.get("kind") as? JsonPrimitive)?.content == "closed"

    /** Asks the [JazzAuth.Bearer] provider for a fresh token ([TokenReason.Refresh]), e.g. after an `Expired` session. */
    public suspend fun refreshSession(): Unit = io { native.refresh() }

    /**
     * Signs the end user out, and closes the app: [session] reads
     * `SignedOut` when this returns.
     */
    public suspend fun signOut() {
        io { native.signOut() }
        // jazz-ffi may end the watches (`closed`) before it delivers the final
        // `signedOut` state, so set it here, as [close] does.
        endWatches()
    }

    /**
     * Links this device account to the user's account with your app's own
     * sign-in ([bearer]): the session becomes that user's. Returns them.
     */
    public suspend fun link(bearer: String): JazzUser = io {
        (SessionState.parse("""{"status":"signedIn","user":${native.link(bearer)}}""") as SessionState.SignedIn).user
    }

    /** Inserts row [id] as [row], or replaces it whole if it exists; returns it. */
    public suspend fun <Row : Any, Insert : Any, P : Patch> upsert(table: Table<Row, Insert, P>, id: String, row: Insert): Row =
        io { decodeStored(table, native.upsert(table.tableName, id, insertJson(table, row))) }

    /**
     * Inserts [row] with [column]'s value streamed from [data] (e.g. a file:
     * `jazz.insertStreaming(Files, NewFile(name = "a.mov"), Files.content, file.inputStream().chunks())`).
     * Nothing is written if [data] fails or the call is cancelled before [data]
     * completes; a cancellation after that may still write it.
     */
    public suspend fun <Row : Any, Insert : Any, P : Patch> insertStreaming(
        table: Table<Row, Insert, P>,
        row: Insert,
        column: Column<*>,
        data: Flow<ByteArray>,
    ): Row = stream(table, column, "insert", null, insertJson(table, row), data)

    /**
     * Replaces [column] of row [id] with a value streamed from [data].
     * Nothing is written if [data] fails or the call is cancelled before [data]
     * completes; a cancellation after that may still write it.
     */
    public suspend fun <Row : Any> updateStreaming(table: Table<Row, *, *>, id: String, column: Column<*>, data: Flow<ByteArray>): Row =
        stream(table, column, "update", id, null, data)

    /**
     * [upsert], with [column]'s value streamed from [data].
     * Nothing is written if [data] fails or the call is cancelled before [data]
     * completes; a cancellation after that may still write it.
     */
    public suspend fun <Row : Any, Insert : Any, P : Patch> upsertStreaming(
        table: Table<Row, Insert, P>,
        id: String,
        row: Insert,
        column: Column<*>,
        data: Flow<ByteArray>,
    ): Row = stream(table, column, "upsert", id, insertJson(table, row), data)

    private suspend fun <Row : Any> stream(
        table: Table<Row, *, *>,
        column: Column<*>,
        kind: String,
        id: String?,
        row: String?,
        data: Flow<ByteArray>,
    ): Row {
        val upload = startHandle(dispatcher, giveBack = { it.abort() }) { native.beginUpload(table.tableName, column.name, kind, id, row) }
        try {
            data.collect { chunk -> io { upload.push(chunk) } }
            return decodeStored(table, io { upload.finish() })
        } catch (e: Throwable) {
            withContext(NonCancellable) { io { runCatching { upload.abort() } } }
            throw e
        }
    }

    /** Brings back deleted row [id] of [table], as it was or with [row] as its content; returns it. */
    public suspend fun <Row : Any, Insert : Any, P : Patch> restore(table: Table<Row, Insert, P>, id: String, row: Insert? = null): Row =
        io { decodeStored(table, native.restore(table.tableName, id, row?.let { insertJson(table, it) })) }

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

    /**
     * Sets text-merge column [column] (`"merge": "text"`, like StagePlan's
     * `tasks.notes`) of row [id] to [text], an edit of [editedFrom], the
     * value you last read: what others changed since is kept, merged.
     */
    public suspend fun editText(table: Table<*, *, *>, id: String, column: Column<out String?>, editedFrom: String, text: String): Unit =
        io { native.editText(table.tableName, id, column.name, editedFrom, text) }

    /**
     * Partial edits of row [id], with an optional [patch] of whole columns,
     * in one write: `jazz.edit(Tasks, id) { text(Tasks.notes, old, new) }`.
     */
    public suspend fun <P : Patch> edit(
        table: Table<*, *, P>,
        id: String,
        patch: (P.() -> Unit)? = null,
        edits: Edits.() -> Unit,
    ): Unit = io {
        val patchJson = patch?.let { patchJson(table, it) }
        native.edit(table.tableName, id, patchJson, Edits().apply(edits).toJson())
    }

    /** Whether this user may read row [id] of [table]: the server's advice, or `null` when it can't say. */
    public suspend fun canRead(table: Table<*, *, *>, id: String): Boolean? = io { native.can(table.tableName, "read", idArgs(id)) }

    /** Whether this user may insert [row] into [table]; `null` when the server can't say. */
    public suspend fun <Row : Any, Insert : Any, P : Patch> canInsert(table: Table<Row, Insert, P>, row: Insert): Boolean? = io {
        native.can(table.tableName, "insert", "{\"row\":${insertJson(table, row)}}")
    }

    /** Whether this user may make [patch] (or any change, without one) to row [id] of [table]. */
    public suspend fun <P : Patch> canUpdate(table: Table<*, *, P>, id: String, patch: (P.() -> Unit)? = null): Boolean? = io {
        val row = patch?.let { ",\"row\":${patchJson(table, it)}" } ?: ""
        native.can(table.tableName, "update", "{\"id\":${JsonPrimitive(id)}$row}")
    }

    /** Whether this user may delete row [id] of [table]. */
    public suspend fun canDelete(table: Table<*, *, *>, id: String): Boolean? = io { native.can(table.tableName, "delete", idArgs(id)) }

    private fun idArgs(id: String): String = "{\"id\":${JsonPrimitive(id)}}"

    /** Returns once every write made so far reached [tier]; throws if the server refused one. */
    public suspend fun waitForSync(tier: WriteTier = WriteTier.Global): Unit = io { native.waitForSync(tier.wire) }

    /**
     * Runs [block] in a transaction: its reads see its own writes, and its
     * writes apply together. By default (as TS's `transaction`) its writes
     * merge with concurrent ones; with [serializable] a commit that conflicts
     * with a concurrent one throws [JazzException.Conflict] instead. It commits when [block] returns (at [tier]:
     * with [WriteTier.Global] it returns once the server accepted it) and
     * rolls back when [block] throws, rethrowing. Returns [block]'s value.
     * Cancelling before the commit starts rolls back; cancelling once it has
     * started doesn't undo it.
     *
     * ```kotlin
     * val moved = jazz.transaction { tx ->
     *     val task = tx.one(Tasks.where(Tasks.id eq id)) ?: return@transaction false
     *     tx.update(Tasks, task.id) { status(TaskStatus.Done) }
     *     true
     * }
     * ```
     */
    public suspend fun <T> transaction(
        tier: WriteTier? = null,
        serializable: Boolean = false,
        block: suspend (Transaction) -> T,
    ): T {
        val native = startHandle(dispatcher, giveBack = { it.rollback() }) { if (serializable) native.begin() else native.beginMergeable() }
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

        /** [Jazz.upsert], in the transaction. */
        public suspend fun <Row : Any, Insert : Any, P : Patch> upsert(table: Table<Row, Insert, P>, id: String, row: Insert): Row =
            run { decodeStored(table, tx.upsert(table.tableName, id, insertJson(table, row))) }

        /** [Jazz.restore], in the transaction. */
        public suspend fun <Row : Any, Insert : Any, P : Patch> restore(table: Table<Row, Insert, P>, id: String, row: Insert? = null): Row =
            run { decodeStored(table, tx.restore(table.tableName, id, row?.let { insertJson(table, it) })) }

        /** [Jazz.editText], in the transaction. */
        public suspend fun editText(table: Table<*, *, *>, id: String, column: Column<out String?>, editedFrom: String, text: String): Unit =
            run { tx.editText(table.tableName, id, column.name, editedFrom, text) }

        /** [Jazz.edit], in the transaction. */
        public suspend fun <P : Patch> edit(
            table: Table<*, *, P>,
            id: String,
            patch: (P.() -> Unit)? = null,
            edits: Edits.() -> Unit,
        ): Unit = run { tx.edit(table.tableName, id, patch?.let { patchJson(table, it) }, Edits().apply(edits).toJson()) }
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
            // A result's key: `$key` where the core sends one (a history or union
            // result, where one row id can be several results), else the row's id.
            for (key in d.removedKeys ?: d.removed) rows.remove(key)
            for (row in d.added + d.updated) rows[resultKey(row)] = row
            emit(query.sort(rows.values.toList()).map { decode(query, it) })
        }
    }.distinctUntilChanged()

    private fun resultKey(row: JsonObject): String =
        ((row["\$key"] ?: row["id"]) as JsonPrimitive).content

    private class RawDelta(
        val added: List<JsonObject>,
        val updated: List<JsonObject>,
        val removed: List<String>,
        val removedKeys: List<String>?,
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
                            removedKeys = o["removedKeys"]?.jsonArray?.map { it.jsonPrimitive.content },
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
    override fun close() {
        native.close()
        endWatches()
    }

    /** The app closed: stop watching, and an end user's session reads as signed out. */
    private fun endWatches() {
        watches.forEach { runCatching { it() } }
        watches.clear()
        sessionFlow?.let { flow -> if (flow.value != null) flow.value = SessionState.SignedOut }
    }

    private suspend fun <T> io(block: () -> T): T = withContext(dispatcher) { block() }
}
