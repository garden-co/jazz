package tools.jazz

/** Why a Jazz call failed. */
public sealed class JazzException(message: String) : Exception(message) {
    /** Refused: invalid input, a permission denied, or not supported yet. */
    public class Refused(message: String) : JazzException(message)

    /** Storage or network failed. */
    public class Io(message: String) : JazzException(message)

    /** The handle was closed. */
    public class Closed : JazzException("the Jazz handle is closed")
}
