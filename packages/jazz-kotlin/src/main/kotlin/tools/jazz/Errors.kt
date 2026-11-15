package tools.jazz

/** Why a Jazz call failed. */
public sealed class JazzException(message: String) : Exception(message) {
    /** Refused: invalid input, a permission denied, or not supported yet. */
    public class Refused(message: String) : JazzException(message)

    /** A permission denied it. */
    public class Permission(message: String) : JazzException(message)

    /** What it asked for isn't supported (yet). */
    public class NotSupported(message: String) : JazzException(message)

    /** It conflicted with a concurrent change (a transaction lost). */
    public class Conflict(message: String) : JazzException(message)

    /** Storage or network failed. */
    public class Io(message: String) : JazzException(message)

    /** A subscription failed after it started (its `Flow` ends with this). */
    public class Subscription(message: String) : JazzException(message)

    /** The handle was closed. */
    public class Closed : JazzException("the Jazz handle is closed")

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
