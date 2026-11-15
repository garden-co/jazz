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

    /** [all] at a read tier: `local`, `localFirst` or `remote`. */
    public fun allAt(queryJson: String, tier: String): String = all(queryJson)
    public fun oneAt(queryJson: String, tier: String): String? = one(queryJson)

    /** Writes that return once they reached a durability tier: `local` or `global`. */
    public fun insertAt(table: String, rowJson: String, tier: String): String = insert(table, rowJson)
    public fun updateAt(table: String, id: String, patchJson: String, tier: String): Unit = update(table, id, patchJson)
    public fun deleteAt(table: String, id: String, tier: String): Unit = delete(table, id)

    public fun subscribeAt(
        queryJson: String,
        tier: String,
        onDelta: (String) -> Unit,
        onError: (String) -> Unit,
    ): () -> Unit = subscribe(queryJson, onDelta, onError)

    /** Returns once every write so far reached [tier]. */
    public fun waitForSync(tier: String) {}

    /** Starts a transaction. */
    public fun begin(): NativeTransaction = throw JazzException.NotSupported("transactions")

    /** Opens native handles from a config JSON (the contract's open config). */
    public fun interface Opener {
        public fun open(configJson: String): Native
    }
}

/** A transaction of the native core: use it from one thread at a time, and end it with [commit] or [rollback]. */
public interface NativeTransaction {
    public fun all(queryJson: String): String
    public fun one(queryJson: String): String?
    public fun insert(table: String, rowJson: String): String
    public fun update(table: String, id: String, patchJson: String)
    public fun delete(table: String, id: String)
    public fun commit(tier: String?)
    public fun rollback()
}
