package tools.jazz

/** Why a Jazz call failed. */
public sealed class JazzException(message: String, public val encryptionCode: String? = null) : Exception(message) {
    /** Refused: invalid input, a permission denied, or not supported yet. */
    public class Refused @JvmOverloads constructor(message: String, encryptionCode: String? = null) :
        JazzException(message, encryptionCode)

    /** A permission denied it. */
    public class Permission(message: String) : JazzException(message)

    /** What it asked for isn't supported (yet). */
    public class NotSupported(message: String) : JazzException(message)

    /** It conflicted with a concurrent change (a transaction lost). */
    public class Conflict(message: String) : JazzException(message)

    /** Storage or network failed. */
    public class Io @JvmOverloads constructor(message: String, encryptionCode: String? = null) :
        JazzException(message, encryptionCode)

    /** A subscription failed after it started (its `Flow` ends with this). */
    public class Subscription(message: String) : JazzException(message)

    /** The handle was closed. */
    public class Closed : JazzException("the Jazz handle is closed")

    /**
     * A write cut short while it waited (the server unreachable, say) that
     * was already committed locally: it stays queued and may still reach
     * the server, so don't retry it. Its outcome comes later, as for any
     * local-first write: a refusal reaches [Jazz.mutationErrors].
     *
     * Thrown in place of the cancellation that cut it short, which is its
     * cause: it escapes `withTimeout` and `withTimeoutOrNull` (which doesn't
     * turn it into `null`), and a cancelled scope's job fails with it.
     */
    public class StillQueued(message: String) : JazzException(message)

    public companion object {
        /** The exception for an error jazz-ffi reports by kind (a subscription's end). */
        public fun of(kind: String, message: String): JazzException = when (kind) {
            "closed" -> Closed()
            "refused" -> Refused(message)
            "io" -> Io(message)
            "permission" -> Permission(message)
            "notSupported" -> NotSupported(message)
            "conflict" -> Conflict(message)
            else -> Subscription(message)
        }
    }
}

/** A write the server refused that nobody waited on: see [Jazz.mutationErrors]. */
public data class MutationError(
    /** The refused transaction's id. */
    val transaction: String,
    val message: String,
    /** Whether permissions denied it (else it failed otherwise, e.g. validation). */
    val denied: Boolean,
    /** Whether it was a serializable transaction that conflicted. */
    val serializable: Boolean,
) {
    internal companion object {
        fun parse(json: String): MutationError {
            val o = jazzJson.parseToJsonElement(json) as kotlinx.serialization.json.JsonObject
            fun str(k: String) = (o[k] as? kotlinx.serialization.json.JsonPrimitive)?.content ?: ""
            fun bool(k: String) = (o[k] as? kotlinx.serialization.json.JsonPrimitive)?.content == "true"
            return MutationError(str("transaction"), str("message"), bool("denied"), bool("serializable"))
        }
    }
}
