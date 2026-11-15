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
    override fun waitForSync(tier: String): Unit = live { handle.waitForSync(tier) }
    override fun editText(table: String, id: String, column: String, editedFrom: String, text: String): Unit =
        live { handle.editText(table, id, column, editedFrom, text) }
    override fun edit(table: String, id: String, patchJson: String?, editsJson: String): Unit =
        live { handle.edit(table, id, patchJson, editsJson) }
    override fun can(table: String, op: String, argsJson: String): Boolean? = live { handle.can(table, op, argsJson) }

    override fun subscribeAt(queryJson: String, tier: String, onDelta: (String) -> Unit, onError: (String, String) -> Unit): () -> Unit =
        listen(onDelta, onError) { handle.subscribeAt(queryJson, tier, it) }

    override fun begin(): NativeTransaction {
        val tx = live { handle.begin() }
        return object : NativeTransaction {
            override fun all(queryJson: String): String = call { tx.all(queryJson) }
            override fun one(queryJson: String): String? = call { tx.one(queryJson) }
            override fun insert(table: String, rowJson: String): String = call { tx.insert(table, rowJson) }
            override fun update(table: String, id: String, patchJson: String): Unit = call { tx.update(table, id, patchJson) }
            override fun delete(table: String, id: String): Unit = call { tx.delete(table, id) }
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
