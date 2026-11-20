@file:OptIn(ExperimentalForeignApi::class, ExperimentalAtomicApi::class)

package tools.jazz

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.StableRef
import kotlinx.cinterop.alloc
import kotlinx.cinterop.asStableRef
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.staticCFunction
import kotlinx.cinterop.toKString
import kotlinx.cinterop.value
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import cnames.structs.JazzHandle
import tools.jazz.cabi.jazz_call
import tools.jazz.cabi.jazz_close
import tools.jazz.cabi.jazz_generate_device_key
import tools.jazz.cabi.jazz_open
import tools.jazz.cabi.jazz_open_user
import tools.jazz.cabi.jazz_string_free
import tools.jazz.cabi.jazz_subscribe
import tools.jazz.cabi.jazz_unsubscribe
import tools.jazz.cabi.jazz_unwatch
import tools.jazz.cabi.jazz_watch
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.experimental.ExperimentalNativeApi
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/**
 * [Native] over jazz-ffi's C ABI (`jazz_ffi.h`, as jazz-go uses it), through
 * cinterop: Kotlin/Native's counterpart of the JVM's UniffiNative. Every
 * method is one `jazz_call` of JSON in, JSON out.
 */
public class CAbiNative private constructor(private var handle: CPointer<JazzHandle>?) : Native {
    /**
     * Calls in flight, and whether [close] ran: the C ABI forbids any call
     * on a handle concurrently with or after `jazz_close` (UniFFI's Arc
     * gives the JVM that for free), so close waits for the calls in flight.
     */
    private val gate = Gate()

    public companion object {
        public fun open(configJson: String): Native = CAbiNative(ffi { err -> jazz_open(configJson, err) })

        /** Opens as an end user whose bearer [tokens] gives (reason → token, or `null` when signed out). */
        public fun openWithTokens(configJson: String, tokens: (String) -> String?): Native {
            // Lives as long as the process: jazz-ffi may ask until jazz_close returns,
            // and a leaked closure per bearer-signed app is cheap. (A real port
            // would dispose it after close.)
            val ref = StableRef.create(TokenSource(tokens))
            return CAbiNative(ffi { err -> jazz_open_user(configJson, tokenCallback, ref.asCPointer(), err) })
        }

        public fun generateDeviceKey(): String = ffiString { err -> jazz_generate_device_key(err) }!!
    }

    private fun call(method: String, args: JsonObject = JsonObject(emptyMap())): String? =
        gate.enter { ffiString { err -> jazz_call(handle, method, args.toString(), err) } }

    /** [call], its result as JSON. */
    private fun callJson(method: String, args: JsonObject = JsonObject(emptyMap())): JsonElement =
        call(method, args)?.let { jazzJson.parseToJsonElement(it) } ?: JsonNull

    private fun query(queryJson: String, tier: String?, tx: String? = null) = buildJsonObject {
        put("query", jazzJson.parseToJsonElement(queryJson))
        tier?.let { put("tier", it) }
        tx?.let { put("tx", it) }
    }

    private fun write(table: String, tier: String?, tx: String?, more: MutableMap<String, JsonElement>.() -> Unit) =
        JsonObject(buildMap {
            put("table", JsonPrimitive(table))
            tier?.let { put("tier", JsonPrimitive(it)) }
            tx?.let { put("tx", JsonPrimitive(it)) }
            more()
        })

    private fun nullable(json: String?): String? = json?.takeUnless { it == "null" }

    override fun account(): String? = (callJson("account") as? JsonPrimitive)?.takeUnless { it is JsonNull }?.content
    override fun all(queryJson: String): String = call("all", query(queryJson, null))!!
    override fun one(queryJson: String): String? = nullable(call("one", query(queryJson, null)))
    override fun allAt(queryJson: String, tier: String): String = call("all", query(queryJson, tier))!!
    override fun oneAt(queryJson: String, tier: String): String? = nullable(call("one", query(queryJson, tier)))

    private fun insertIn(tx: String?, tier: String?, table: String, rowJson: String) =
        call("insert", write(table, tier, tx) { put("row", jazzJson.parseToJsonElement(rowJson)) })!!
    private fun updateIn(tx: String?, tier: String?, table: String, id: String, patchJson: String) {
        call("update", write(table, tier, tx) { put("id", JsonPrimitive(id)); put("patch", jazzJson.parseToJsonElement(patchJson)) })
    }
    private fun deleteIn(tx: String?, tier: String?, table: String, id: String) {
        call("delete", write(table, tier, tx) { put("id", JsonPrimitive(id)) })
    }
    private fun upsertIn(tx: String?, table: String, id: String, rowJson: String) =
        call("upsert", write(table, null, tx) { put("id", JsonPrimitive(id)); put("row", jazzJson.parseToJsonElement(rowJson)) })!!
    private fun restoreIn(tx: String?, table: String, id: String, patchJson: String?) =
        call("restore", write(table, null, tx) {
            put("id", JsonPrimitive(id))
            patchJson?.let { put("patch", jazzJson.parseToJsonElement(it)) }
        })!!
    private fun editTextIn(tx: String?, table: String, id: String, column: String, editedFrom: String, text: String) {
        call("editText", write(table, null, tx) {
            put("id", JsonPrimitive(id)); put("column", JsonPrimitive(column))
            put("editedFrom", JsonPrimitive(editedFrom)); put("text", JsonPrimitive(text))
        })
    }
    private fun editIn(tx: String?, table: String, id: String, patchJson: String?, editsJson: String) {
        call("edit", write(table, null, tx) {
            put("id", JsonPrimitive(id))
            patchJson?.let { put("patch", jazzJson.parseToJsonElement(it)) }
            put("edits", jazzJson.parseToJsonElement(editsJson))
        })
    }

    override fun insert(table: String, rowJson: String): String = insertIn(null, null, table, rowJson)
    override fun update(table: String, id: String, patchJson: String): Unit = updateIn(null, null, table, id, patchJson)
    override fun delete(table: String, id: String): Unit = deleteIn(null, null, table, id)
    override fun insertAt(table: String, rowJson: String, tier: String): String = insertIn(null, tier, table, rowJson)
    override fun updateAt(table: String, id: String, patchJson: String, tier: String): Unit = updateIn(null, tier, table, id, patchJson)
    override fun deleteAt(table: String, id: String, tier: String): Unit = deleteIn(null, tier, table, id)
    override fun upsert(table: String, id: String, rowJson: String): String = upsertIn(null, table, id, rowJson)
    override fun restore(table: String, id: String, patchJson: String?): String = restoreIn(null, table, id, patchJson)
    override fun editText(table: String, id: String, column: String, editedFrom: String, text: String): Unit =
        editTextIn(null, table, id, column, editedFrom, text)
    override fun edit(table: String, id: String, patchJson: String?, editsJson: String): Unit = editIn(null, table, id, patchJson, editsJson)

    override fun can(table: String, op: String, argsJson: String): Boolean? {
        val args = jazzJson.parseToJsonElement(argsJson) as JsonObject
        val result = callJson("can", JsonObject(args + mapOf("table" to JsonPrimitive(table), "op" to JsonPrimitive(op))))
        return (result as? JsonPrimitive)?.booleanOrNull
    }

    override fun waitForSync(tier: String) {
        call("waitForSync", buildJsonObject { put("tier", tier) })
    }
    override fun sessionState(): String? = nullable(call("session"))
    override fun syncStatus(): String = call("syncStatus")!!
    override fun refresh() {
        call("refresh")
    }
    override fun link(bearer: String): String = call("link", buildJsonObject { put("bearer", bearer) })!!
    override fun disconnect() {
        call("disconnect")
    }
    override fun reconnect() {
        call("reconnect")
    }

    override fun begin(): NativeTransaction = transaction(serializable = true)
    override fun beginMergeable(): NativeTransaction = transaction(serializable = false)

    private fun transaction(serializable: Boolean): NativeTransaction {
        val tx = (callJson("txBegin", buildJsonObject { put("serializable", serializable) }) as JsonObject)["tx"]!!.jsonPrimitive.content
        return object : NativeTransaction {
            override fun all(queryJson: String): String = call("all", query(queryJson, null, tx))!!
            override fun one(queryJson: String): String? = nullable(call("one", query(queryJson, null, tx)))
            override fun insert(table: String, rowJson: String): String = insertIn(tx, null, table, rowJson)
            override fun update(table: String, id: String, patchJson: String): Unit = updateIn(tx, null, table, id, patchJson)
            override fun delete(table: String, id: String): Unit = deleteIn(tx, null, table, id)
            override fun upsert(table: String, id: String, rowJson: String): String = upsertIn(tx, table, id, rowJson)
            override fun restore(table: String, id: String, patchJson: String?): String = restoreIn(tx, table, id, patchJson)
            override fun editText(table: String, id: String, column: String, editedFrom: String, text: String): Unit =
                editTextIn(tx, table, id, column, editedFrom, text)
            override fun edit(table: String, id: String, patchJson: String?, editsJson: String): Unit =
                editIn(tx, table, id, patchJson, editsJson)
            override fun commit(tier: String?) {
                call("txCommit", buildJsonObject { put("tx", tx); tier?.let { put("tier", it) } })
            }
            override fun rollback() {
                call("txRollback", buildJsonObject { put("tx", tx) })
            }
        }
    }

    @OptIn(ExperimentalEncodingApi::class)
    override fun beginUpload(table: String, column: String, kind: String, id: String?, rowJson: String?): NativeUpload {
        val upload = (callJson("uploadBegin", write(table, null, null) {
            put("column", JsonPrimitive(column)); put("kind", JsonPrimitive(kind))
            id?.let { put("id", JsonPrimitive(it)) }
            rowJson?.let { put("row", jazzJson.parseToJsonElement(it)) }
        }) as JsonObject)["upload"]!!
        return object : NativeUpload {
            override fun push(bytes: ByteArray) {
                call("uploadPush", buildJsonObject { put("upload", upload); put("bytes", Base64.encode(bytes)) })
            }
            override fun finish(): String = call("uploadFinish", buildJsonObject { put("upload", upload) })!!
            override fun abort() {
                call("uploadAbort", buildJsonObject { put("upload", upload) })
            }
        }
    }

    override fun subscribe(queryJson: String, onDelta: (String) -> Unit, onError: (String, String) -> Unit): () -> Unit =
        subscribeWith(queryJson, onDelta, onError)

    override fun subscribeAt(queryJson: String, tier: String, onDelta: (String) -> Unit, onError: (String, String) -> Unit): () -> Unit =
        subscribeWith(
            buildJsonObject { put("query", jazzJson.parseToJsonElement(queryJson)); put("tier", tier) }.toString(),
            onDelta,
            onError,
        )

    /**
     * jazz_subscribe with a [Listener] behind a StableRef as its ctx: the
     * callback runs on a Rust thread, which Kotlin/Native attaches to its
     * runtime on first use. The ref is disposed once jazz_unsubscribe
     * returns, when no callback runs or will start.
     */
    private fun subscribeWith(queryJson: String, onDelta: (String) -> Unit, onError: (String, String) -> Unit): () -> Unit {
        val ref = StableRef.create(Listener(onDelta, onError))
        val sub = try {
            gate.enter { ffi { err -> jazz_subscribe(handle, queryJson, listenerCallback, ref.asCPointer(), err) } }
        } catch (e: Throwable) {
            ref.dispose()
            throw e
        }
        val done = AtomicInt(0)
        return {
            if (done.compareAndSet(0, 1)) {
                jazz_unsubscribe(sub)
                ref.dispose()
            }
        }
    }

    override fun watch(sync: Boolean, onChange: (String) -> Unit): () -> Unit =
        watchWith(if (sync) "sync" else "session", onChange)

    override fun watchMutationErrors(onError: (String) -> Unit): () -> Unit = watchWith("mutationErrors", onError)

    private fun watchWith(what: String, onChange: (String) -> Unit): () -> Unit {
        // A watch's last call is kind "error" with {"kind":"closed"}; [Jazz] reads it as such.
        val ref = StableRef.create(Listener(onDelta = onChange, onError = { _, _ -> }, passErrors = onChange))
        val watch = try {
            gate.enter { ffi { err -> jazz_watch(handle, what, listenerCallback, ref.asCPointer(), err) } }
        } catch (e: Throwable) {
            ref.dispose()
            throw e
        }
        val done = AtomicInt(0)
        return {
            if (done.compareAndSet(0, 1)) {
                jazz_unwatch(watch)
                ref.dispose()
            }
        }
    }

    override fun signOut() {
        if (!gate.close()) return
        try {
            ffiString { err -> jazz_call(handle, "signOut", "{}", err) }
        } finally {
            jazz_close(handle)
            handle = null
        }
    }

    override fun deleteClientStorage() {
        gate.enter { ffiString { err -> jazz_call(handle, "deleteClientStorage", "{}", err) } }
        // Deleted: the core closed the app; free the handle.
        if (gate.close()) {
            jazz_close(handle)
            handle = null
        }
    }

    /** Closes the app once; later calls throw [JazzException.Closed], a second close does nothing. */
    override fun close() {
        if (!gate.close()) return
        jazz_close(handle)
        handle = null
    }
}

/** A subscription's or watch's Kotlin side, behind the C callback's ctx. */
private class Listener(
    val onDelta: (String) -> Unit,
    val onError: (String, String) -> Unit,
    /** A watch: its error JSON goes to [onDelta]'s counterpart as it is. */
    val passErrors: ((String) -> Unit)? = null,
)

private class TokenSource(val tokens: (String) -> String?)

/** The one C callback every subscription and watch shares; ctx tells them apart. */
@OptIn(ExperimentalNativeApi::class)
private val listenerCallback = staticCFunction { ctx: COpaquePointer?, kind: CPointer<ByteVar>?, json: CPointer<ByteVar>? ->
    // An exception escaping into Rust would abort the process: report it instead.
    try {
        val listener = ctx!!.asStableRef<Listener>().get()
        val text = json?.toKString() ?: "null"
        if (kind?.toKString() == "error") {
            listener.passErrors?.invoke(text) ?: run {
                val o = jazzJson.parseToJsonElement(text) as? JsonObject
                val k = (o?.get("kind") as? JsonPrimitive)?.content ?: "refused"
                val m = (o?.get("message") as? JsonPrimitive)?.content ?: text
                listener.onError(k, m)
            }
        } else {
            listener.onDelta(text)
        }
    } catch (e: Throwable) {
        println("jazz: a subscription callback threw: $e")
    }
}

/** The token callback: 0 with *token malloc'ed (or NULL: signed out), non-zero on a failed fetch. */
private val tokenCallback = staticCFunction { ctx: COpaquePointer?, reason: CPointer<ByteVar>?, token: CPointer<CPointerVar<ByteVar>>? ->
    try {
        val t = ctx!!.asStableRef<TokenSource>().get().tokens(reason?.toKString() ?: "connect")
        token!!.pointed.value = t?.let { strdupC(it) }
        0
    } catch (e: Throwable) {
        token?.pointed?.value = strdupC(e.message ?: e.toString())
        1
    }
}

private fun strdupC(s: String): CPointer<ByteVar>? = platform.posix.strdup(s)

/** Runs [f] with an error out-pointer; its NULL result becomes the [JazzException] jazz-ffi stored. */
internal inline fun <T : kotlinx.cinterop.CPointed> ffi(f: (CPointer<CPointerVar<ByteVar>>) -> CPointer<T>?): CPointer<T> = memScoped {
    val err = alloc<CPointerVar<ByteVar>>()
    err.value = null
    val result = f(err.ptr)
    result ?: throw takeError(err.value)
}

/** [ffi] for a call returning an owned string: its text (freed here), or `null` for JSON null... never: a NULL return is an error. */
internal inline fun ffiString(f: (CPointer<CPointerVar<ByteVar>>) -> CPointer<ByteVar>?): String? = memScoped {
    val err = alloc<CPointerVar<ByteVar>>()
    err.value = null
    val result = f(err.ptr)
    if (result == null) throw takeError(err.value)
    try {
        result.toKString()
    } finally {
        jazz_string_free(result)
    }
}

/** The error jazz-ffi stored in *err (freed here), as a [JazzException]. */
internal fun takeError(err: CPointer<ByteVar>?): JazzException {
    if (err == null) return JazzException.Refused("jazz-ffi failed without an error")
    val text = try { err.toKString() } finally { jazz_string_free(err) }
    val o = runCatching { jazzJson.parseToJsonElement(text) as JsonObject }.getOrNull()
    val kind = (o?.get("kind") as? JsonPrimitive)?.content ?: "refused"
    val message = (o?.get("message") as? JsonPrimitive)?.content ?: text
    return when (kind) {
        "closed" -> JazzException.Closed()
        "refused", "io", "permission", "notSupported", "conflict" -> JazzException.of(kind, message)
        // Kinds a newer jazz-ffi adds (noAccount, unsynced, …): refused, with the message, as UniffiNative does.
        else -> JazzException.Refused(message)
    }
}

/** Counts calls in flight on a handle so that closing waits for them and later calls fail as closed. */
private class Gate {
    /** Calls in flight, or [CLOSED] plus those still running once closing began. */
    private val state = AtomicInt(0)

    inline fun <T> enter(block: () -> T): T {
        while (true) {
            val s = state.load()
            if (s >= CLOSED) throw JazzException.Closed()
            if (state.compareAndSet(s, s + 1)) break
        }
        try {
            return block()
        } finally {
            state.addAndFetch(-1)
        }
    }

    /** Marks it closed and waits for calls in flight; `false` if it was already closed. */
    fun close(): Boolean {
        while (true) {
            val s = state.load()
            if (s >= CLOSED) return false
            if (state.compareAndSet(s, s + CLOSED)) break
        }
        while (state.load() != CLOSED) platform.posix.sched_yield()
        return true
    }

    private companion object {
        const val CLOSED = 1 shl 30
    }
}
