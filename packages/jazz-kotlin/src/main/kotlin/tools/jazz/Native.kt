package tools.jazz

/**
 * The untyped JSON-in/JSON-out surface of `jazz-ffi` that [Jazz] wraps.
 * [UniffiNative] implements it over the UniFFI bindings; tests may pass
 * their own.
 */
public interface Native : AutoCloseable {
    /** The session user's account id, or `null` for a client. */
    public fun account(): String?
    public fun all(queryJson: String): String
    public fun one(queryJson: String): String?
    public fun insert(table: String, rowJson: String): String
    public fun update(table: String, id: String, patchJson: String)
    public fun delete(table: String, id: String)

    /** Subscribes; the returned function cancels. */
    public fun subscribe(
        queryJson: String,
        onDelta: (String) -> Unit,
        onError: (String) -> Unit,
    ): () -> Unit

    /** Opens native handles from a config JSON (the contract's open config). */
    public fun interface Opener {
        public fun open(configJson: String): Native
    }
}
