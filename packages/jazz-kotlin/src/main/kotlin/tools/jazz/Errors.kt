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
}
