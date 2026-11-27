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

    /**
     * Subscribes; the returned function cancels. [onError] gets the error
     * that ends it: its kind (`closed`, `refused`, `io`, `permission`,
     * `notSupported`, `conflict`) and message.
     */
    public fun subscribe(
        queryJson: String,
        onDelta: (String) -> Unit,
        onError: (kind: String, message: String) -> Unit,
    ): () -> Unit

    /** [all] at a read tier: `local`, `localFirst` or `remote`. */
    public fun allAt(queryJson: String, tier: String): String = all(queryJson)
    public fun oneAt(queryJson: String, tier: String): String? = one(queryJson)

    /** Writes that return once they reached a durability tier: `local` or `global`. */
    public fun insertAt(table: String, rowJson: String, tier: String): String = insert(table, rowJson)
    public fun updateAt(table: String, id: String, patchJson: String, tier: String): Unit = update(table, id, patchJson)
    public fun deleteAt(table: String, id: String, tier: String): Unit = delete(table, id)

    /** [upsert] at a durability tier. */
    public fun upsertAt(table: String, id: String, rowJson: String, tier: String): String = throw JazzException.NotSupported("upsert at a tier")

    public fun subscribeAt(
        queryJson: String,
        tier: String,
        onDelta: (String) -> Unit,
        onError: (kind: String, message: String) -> Unit,
    ): () -> Unit = subscribe(queryJson, onDelta, onError)

    /** Sets text-merge column [column] to [text], an edit of [editedFrom]. */
    public fun editText(table: String, id: String, column: String, editedFrom: String, text: String): Unit =
        throw JazzException.NotSupported("text edits")

    /** Partial edits ([editsJson]: text, splice and jsonSet edits), with an optional patch, in one write. */
    public fun edit(table: String, id: String, patchJson: String?, editsJson: String): Unit =
        throw JazzException.NotSupported("partial edits")

    /** The server's advice on whether `op` would be allowed: `true`, `false` or `null` (unknown). */
    public fun can(table: String, op: String, argsJson: String): Boolean? = null

    /** Returns once every write so far reached [tier]. */
    public fun waitForSync(tier: String) {}

    /** Starts a serializable transaction: a commit that conflicts with a concurrent one fails. */
    public fun begin(): NativeTransaction = throw JazzException.NotSupported("transactions")

    /** Starts a mergeable transaction (TS's default): its writes merge with concurrent ones. */
    public fun beginMergeable(): NativeTransaction = begin()

    /** Stops syncing until [reconnect]; reads and writes go on locally. */
    public fun disconnect(): Unit = throw JazzException.NotSupported("disconnect")

    public fun reconnect(): Unit = throw JazzException.NotSupported("reconnect")

    /** Closes the app and deletes its local store. */
    public fun deleteClientStorage(): Unit = throw JazzException.NotSupported("deleting client storage")

    /** Calls [onError] with each refused write nobody waited on (JSON); returns a cancel. */
    public fun watchMutationErrors(onError: (String) -> Unit): () -> Unit = {}

    /** The end user's session as JSON, or `null` for a local app or a backend. */
    public fun sessionState(): String? = null

    /** The sync status as JSON: `{"link": {"state": …}}`. */
    public fun syncStatus(): String = """{"link":{"state":"localOnly"},"synced":true}"""

    /** Calls [onChange] with each session (or, with [sync], sync status) state, the current one first; returns a cancel. */
    public fun watch(sync: Boolean, onChange: (String) -> Unit): () -> Unit = {}

    /** Links this device account to the app's sign-in ([bearer]); the user as JSON. */
    public fun link(bearer: String): String = throw JazzException.NotSupported("linking accounts")

    /** Inserts or replaces row [id] with [rowJson] (a whole row); returns it. */
    public fun upsert(table: String, id: String, rowJson: String): String = throw JazzException.NotSupported("upsert")

    /** Brings back deleted row [id], as it was or with [patchJson] (a whole row) as its content; returns it. */
    public fun restore(table: String, id: String, patchJson: String?): String = throw JazzException.NotSupported("restore")

    /** Starts a streaming write of [column]'s value: [kind] `insert`, `update` or `upsert`. */
    public fun beginUpload(table: String, column: String, kind: String, id: String?, rowJson: String?): NativeUpload =
        throw JazzException.NotSupported("streaming uploads")

    /** The permissions bundle a backend published, or `null` for a client. */
    public fun bundle(): String? = null

    /** Asks the token provider for a fresh token. */
    public fun refresh() {}

    /** Signs the end user out, and closes the app. */
    public fun signOut(): Unit = close()

    public fun memberAdd(table: String, id: String, accountId: String, tier: String?): String =
        throw JazzException.NotSupported("adding space members")
    public fun memberList(table: String, id: String): String =
        throw JazzException.NotSupported("listing space members")
    public fun memberRemove(table: String, id: String, accountId: String): ULong =
        throw JazzException.NotSupported("removing space members")

    /** One end-to-end encryption operation by name (see the README), its arguments and result as JSON. */
    public fun encryptionCall(op: String, argsJson: String): String =
        throw JazzException.NotSupported("end-to-end encryption through this native core")

    /**
     * A call whose waits [NativeCall.cancel] can end, or `null` where calls
     * can't be cut short. Calls made inside [NativeCall.run], on that
     * thread, obey it.
     */
    public fun newCall(): NativeCall? = null

    /** Opens native handles from a config JSON (the contract's open config). */

    public fun interface Opener {
        public fun open(configJson: String): Native

        /** Opens as an end user signing in with a bearer [tokens] gives (reason → token or `null`). */
        public fun openWithTokens(configJson: String, tokens: (String) -> String?): Native =
            throw JazzException.NotSupported("bearer sign-in through this opener")

        /**
         * Opens an app that declares end-to-end encryption, with [store] answering the device's
         * private store requests; [tokens] for a bearer sign-in.
         */
        public fun openWithStore(configJson: String, store: PrivateStore, tokens: ((String) -> String?)?): Native =
            throw JazzException.NotSupported("end-to-end encryption through this opener")
    }
}

/** A transaction of the native core: use it from one thread at a time, and end it with [commit] or [rollback]. */
public interface NativeTransaction {
    public fun all(queryJson: String): String
    public fun one(queryJson: String): String?
    public fun insert(table: String, rowJson: String): String
    public fun update(table: String, id: String, patchJson: String)
    public fun delete(table: String, id: String)
    public fun upsert(table: String, id: String, rowJson: String): String = throw JazzException.NotSupported("upsert")
    public fun restore(table: String, id: String, patchJson: String?): String = throw JazzException.NotSupported("restore")
    public fun editText(table: String, id: String, column: String, editedFrom: String, text: String): Unit =
        throw JazzException.NotSupported("text edits")
    public fun edit(table: String, id: String, patchJson: String?, editsJson: String): Unit =
        throw JazzException.NotSupported("partial edits")
    public fun commit(tier: String?)
    public fun rollback()
}

/** A streaming write of a large value: [push] its bytes in order, then [finish] (returns the row) or [abort]. */
public interface NativeUpload {
    public fun push(bytes: ByteArray)
    public fun finish(): String
    public fun abort()
}

/** One call that can be cut short: see [Native.newCall]. */
public interface NativeCall {
    /** Runs [block], whose calls on this [Native] obey this one, then frees it. */
    public fun <T> run(block: () -> T): T

    /** Ends its waits now: a call waiting fails (and one made later does nothing). */
    public fun cancel()
}
