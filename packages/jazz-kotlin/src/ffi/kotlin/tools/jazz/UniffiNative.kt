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
            }
        }
    }

    override fun account(): String? = live { handle.account() }
    override fun all(queryJson: String): String = live { handle.all(queryJson) }
    override fun one(queryJson: String): String? = live { handle.one(queryJson) }
    override fun insert(table: String, rowJson: String): String = live { handle.insert(table, rowJson) }
    override fun update(table: String, id: String, patchJson: String): Unit = live { handle.update(table, id, patchJson) }
    override fun delete(table: String, id: String): Unit = live { handle.delete(table, id) }

    override fun subscribe(queryJson: String, onDelta: (String) -> Unit, onError: (String) -> Unit): () -> Unit {
        val subscription = live {
            handle.subscribe(
                queryJson,
                object : SubscriptionListener {
                    override fun onDelta(deltaJson: String) = onDelta(deltaJson)
                    override fun onError(message: String) = onError(message)
                },
            )
        }
        return {
            try {
                subscription.cancel()
            } finally {
                subscription.destroy()
            }
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
