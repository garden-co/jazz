package tools.jazz

import uniffi.jazz_ffi.JazzHandle
import uniffi.jazz_ffi.SubscriptionListener
import uniffi.jazz_ffi.JazzException as FfiException

/** [Native] over the Kotlin bindings uniffi-bindgen generates for jazz-ffi. */
public class UniffiNative private constructor(private val handle: JazzHandle) : Native {
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

    override fun account(): String? = call { handle.account() }
    override fun all(queryJson: String): String = call { handle.all(queryJson) }
    override fun one(queryJson: String): String? = call { handle.one(queryJson) }
    override fun insert(table: String, rowJson: String): String = call { handle.insert(table, rowJson) }
    override fun update(table: String, id: String, patchJson: String): Unit = call { handle.update(table, id, patchJson) }
    override fun delete(table: String, id: String): Unit = call { handle.delete(table, id) }

    override fun subscribe(queryJson: String, onDelta: (String) -> Unit, onError: (String) -> Unit): () -> Unit {
        val subscription = call {
            handle.subscribe(
                queryJson,
                object : SubscriptionListener {
                    override fun onDelta(deltaJson: String) = onDelta(deltaJson)
                    override fun onError(message: String) = onError(message)
                },
            )
        }
        return { subscription.cancel() }
    }

    override fun close() {
        try {
            call { handle.shutdown() }
        } finally {
            handle.destroy()
        }
    }
}
