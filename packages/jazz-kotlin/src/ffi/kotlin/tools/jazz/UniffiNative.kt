package tools.jazz

import uniffi.jazz_ffi.JazzHandle
import uniffi.jazz_ffi.SubscriptionListener
import uniffi.jazz_ffi.JazzException as FfiException

/** [Native] over the Kotlin bindings uniffi-bindgen generates for jazz-ffi. */
public class UniffiNative private constructor(private val handle: JazzHandle) : Native {
    private val closed = java.util.concurrent.atomic.AtomicBoolean(false)

    /** [block] on the handle, or [JazzException.Closed] once [close] ran. */
    private inline fun <T> live(block: () -> T): T {
        if (closed.get()) throw JazzException.Closed()
        return call(block)
    }

    public companion object {
        @JvmStatic
        public fun open(configJson: String): Native = UniffiNative(call { JazzHandle.open(configJson) })

        @JvmStatic
        public fun openWithTokens(configJson: String, tokens: (String) -> String?): Native {
            val provider = object : uniffi.jazz_ffi.TokenProvider {
                // A provider that throws (offline, the sign-in server down) is an error the
                // session retries, not a sign-out: only returning null signs the user out.
                override fun token(reason: String): String? = try {
                    tokens(reason)
                } catch (e: Throwable) {
                    throw FfiException.Io(e.message ?: e.toString())
                }
            }
            return UniffiNative(call { JazzHandle.openWithTokens(configJson, provider) })
        }

        @JvmStatic
        public fun generateDeviceKey(): String = call { uniffi.jazz_ffi.generateDeviceKey() }

        private inline fun <T> call(block: () -> T): T = try {
            block()
        } catch (e: FfiException) {
            throw when (e) {
                is FfiException.Refused -> JazzException.Refused(e.reason)
                is FfiException.Io -> JazzException.Io(e.reason)
                is FfiException.Closed -> JazzException.Closed()
                is FfiException.Permission -> JazzException.Permission(e.reason)
                is FfiException.NotSupported -> JazzException.NotSupported(e.reason)
                is FfiException.Conflict -> JazzException.Conflict(e.reason)
                // Kinds a newer jazz-ffi adds (e.g. NoAccount, Unsynced): refused, with its message.
                else -> JazzException.Refused(e.message ?: e.toString())
            }
        }
    }

    override fun account(): String? = live { handle.account() }
    override fun all(queryJson: String): String = live { handle.all(queryJson) }
    override fun one(queryJson: String): String? = live { handle.one(queryJson) }
    override fun insert(table: String, rowJson: String): String = live { handle.insert(table, rowJson) }
    override fun update(table: String, id: String, patchJson: String): Unit = live { handle.update(table, id, patchJson) }
    override fun delete(table: String, id: String): Unit = live { handle.delete(table, id) }

    override fun subscribe(queryJson: String, onDelta: (String) -> Unit, onError: (String, String) -> Unit): () -> Unit =
        listen(onDelta, onError) { handle.subscribe(queryJson, it) }

    private inline fun listen(
        crossinline onDelta: (String) -> Unit,
        crossinline onError: (String, String) -> Unit,
        subscribe: (SubscriptionListener) -> uniffi.jazz_ffi.Subscription,
    ): () -> Unit {
        val listener = object : SubscriptionListener {
            override fun onDelta(deltaJson: String) = onDelta(deltaJson)
            override fun onError(kind: String, message: String) = onError(kind, message)
        }
        val subscription = live { subscribe(listener) }
        return {
            try {
                subscription.cancel()
            } finally {
                subscription.destroy()
            }
        }
    }

    override fun allAt(queryJson: String, tier: String): String = live { handle.allAt(queryJson, tier) }
    override fun oneAt(queryJson: String, tier: String): String? = live { handle.oneAt(queryJson, tier) }
    override fun insertAt(table: String, rowJson: String, tier: String): String = live { handle.insertAt(table, rowJson, tier) }
    override fun updateAt(table: String, id: String, patchJson: String, tier: String): Unit =
        live { handle.updateAt(table, id, patchJson, tier) }
    override fun deleteAt(table: String, id: String, tier: String): Unit = live { handle.deleteAt(table, id, tier) }
    override fun upsertAt(table: String, id: String, rowJson: String, tier: String): String =
        live { handle.upsertAt(table, id, rowJson, tier) }
    override fun waitForSync(tier: String): Unit = live { handle.waitForSync(tier) }
    override fun sessionState(): String? = live { handle.sessionState() }
    override fun syncStatus(): String = live { handle.syncStatus() }
    override fun refresh(): Unit = live { handle.refresh() }
    override fun bundle(): String? = live { handle.bundle() }

    override fun beginUpload(table: String, column: String, kind: String, id: String?, rowJson: String?): NativeUpload {
        val upload = live { handle.beginUpload(table, column, kind, id, rowJson) }
        return object : NativeUpload {
            override fun push(bytes: ByteArray): Unit = call { upload.push(bytes) }
            override fun finish(): String = try { call { upload.finish() } } finally { upload.destroy() }
            override fun abort(): Unit = try { call { upload.abort() } } finally { upload.destroy() }
        }
    }
    override fun link(bearer: String): String = live { handle.link(bearer) }
    override fun upsert(table: String, id: String, rowJson: String): String = live { handle.upsert(table, id, rowJson) }
    override fun restore(table: String, id: String, patchJson: String?): String = live { handle.restore(table, id, patchJson) }

    override fun signOut() {
        if (!closed.compareAndSet(false, true)) return
        try {
            call { handle.signOut() }
        } finally {
            handle.destroy()
        }
    }

    override fun watch(sync: Boolean, onChange: (String) -> Unit): () -> Unit {
        val listener = object : uniffi.jazz_ffi.StateListener {
            override fun onChange(stateJson: String) = onChange(stateJson)
        }
        val watch = live { if (sync) handle.watchSync(listener) else handle.watchSession(listener) }
        return {
            try {
                watch.cancel()
            } finally {
                watch.destroy()
            }
        }
    }
    override fun editText(table: String, id: String, column: String, editedFrom: String, text: String): Unit =
        live { handle.editText(table, id, column, editedFrom, text) }
    override fun edit(table: String, id: String, patchJson: String?, editsJson: String): Unit =
        live { handle.edit(table, id, patchJson, editsJson) }
    override fun can(table: String, op: String, argsJson: String): Boolean? = live { handle.can(table, op, argsJson) }

    override fun subscribeAt(queryJson: String, tier: String, onDelta: (String) -> Unit, onError: (String, String) -> Unit): () -> Unit =
        listen(onDelta, onError) { handle.subscribeAt(queryJson, tier, it) }

    override fun begin(): NativeTransaction = transaction(live { handle.begin() })

    override fun beginMergeable(): NativeTransaction = transaction(live { handle.beginMergeable() })

    override fun disconnect(): Unit = live { handle.disconnect() }
    override fun reconnect(): Unit = live { handle.reconnect() }

    override fun deleteClientStorage() {
        if (closed.get()) throw JazzException.Closed()
        try {
            call { handle.deleteClientStorage() }
        } catch (e: JazzException.Refused) {
            // Refused (e.g. another open app holds the store): the app stays open.
            throw e
        } catch (e: Throwable) {
            if (closed.compareAndSet(false, true)) handle.destroy()
            throw e
        }
        // Deleted: the core closed the app.
        if (closed.compareAndSet(false, true)) handle.destroy()
    }

    override fun watchMutationErrors(onError: (String) -> Unit): () -> Unit {
        val listener = object : uniffi.jazz_ffi.StateListener {
            override fun onChange(stateJson: String) = onError(stateJson)
        }
        val watch = live { handle.watchMutationErrors(listener) }
        return {
            try {
                watch.cancel()
            } finally {
                watch.destroy()
            }
        }
    }

    private fun transaction(tx: uniffi.jazz_ffi.Transaction): NativeTransaction {
        return object : NativeTransaction {
            override fun all(queryJson: String): String = call { tx.all(queryJson) }
            override fun one(queryJson: String): String? = call { tx.one(queryJson) }
            override fun insert(table: String, rowJson: String): String = call { tx.insert(table, rowJson) }
            override fun update(table: String, id: String, patchJson: String): Unit = call { tx.update(table, id, patchJson) }
            override fun delete(table: String, id: String): Unit = call { tx.delete(table, id) }
            override fun upsert(table: String, id: String, rowJson: String): String = call { tx.upsert(table, id, rowJson) }
            override fun restore(table: String, id: String, patchJson: String?): String = call { tx.restore(table, id, patchJson) }
            override fun editText(table: String, id: String, column: String, editedFrom: String, text: String): Unit =
                call { tx.editText(table, id, column, editedFrom, text) }
            override fun edit(table: String, id: String, patchJson: String?, editsJson: String): Unit =
                call { tx.edit(table, id, patchJson, editsJson) }
            override fun commit(tier: String?): Unit = try { call { tx.commit(tier) } } finally { tx.destroy() }
            override fun rollback(): Unit = try { call { tx.rollback() } } finally { tx.destroy() }
        }
    }

    /** Closes the app once; later calls (and a second close) do nothing or throw [JazzException.Closed]. */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        try {
            call { handle.shutdown() }
        } finally {
            handle.destroy()
        }
    }
}
