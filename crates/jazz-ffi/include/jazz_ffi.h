/*
 * jazz_ffi.h: the C ABI of crates/jazz-ffi, for Go (cgo) and C.
 * Hand-written; keep it in step with src/c_abi.rs.
 *
 * A JSON-in/JSON-out layer over a Jazz app opened from its jazz.app.json:
 * rows, queries and deltas are plain JSON typed by the app's schema (see
 * crates/jazz-ffi/README.md).
 *
 * Strings are UTF-8 and NUL-terminated. Every string a function returns,
 * and every error it stores in *err, belongs to the caller: free it with
 * jazz_string_free. On failure a function returns NULL and, when err is not
 * NULL, stores {"kind":"refused"|"io"|"closed","message":"..."} in *err
 * (with "contract": 2 in the open config, also "permission" (the app's
 * permissions refuse it), "notSupported" (not supported yet) and "conflict"
 * (a serializable transaction lost to another write; retry it))
 * (*err is set to NULL on success). Functions block until done. Calls on
 * one handle may come from any threads at once, except jazz_close: no call
 * on that handle may run concurrently with it or follow it. Likewise no
 * call may use a subscription concurrently with or after its
 * jazz_unsubscribe.
 */
#ifndef JAZZ_FFI_H
#define JAZZ_FFI_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct JazzHandle JazzHandle;
typedef struct JazzSubscription JazzSubscription;

/*
 * Called on a thread of the subscription's own. kind is "delta" (json is
 * the delta JSON; the first has "reset": true and the full result) or
 * "error" (json is the error JSON; no callback follows). Kind "closed"
 * means this subscription ended: its app closed, or its stream ended for
 * another reason (the client stopping, the server ending the query). Both strings are
 * valid only during the call. ctx is passed through unchanged and must be
 * usable from that thread.
 */
typedef void (*JazzCallback)(void *ctx, const char *kind, const char *json);

/*
 * Opens an app. config_json: { "app": "<jazz.app.json text>",
 * "permissions"?: "<jazz.permissions.json text>", "appId"?, "serverUrl"?,
 * "dataDir"?, "storage"?: "memory"|"persistent", "backendSecret"?,
 * "adminSecret"?, "session"?: { "issuer"?: "jazz:backend", "subject" } }.
 * With permissions it opens as the app's backend acting as the session's
 * user; without, as a local client.
 */
JazzHandle *jazz_open(const char *config_json, char **err);

/*
 * Calls method with args_json and returns its result as JSON:
 *   "all"     {"query": Q}                    -> [row, ...]
 *   "one"     {"query": Q}                    -> row | null
 *   "insert"  {"table": T, "row": R}          -> the inserted row, with "id"
 *   "update"  {"table": T, "id": I, "patch": P} -> null
 *   "delete"  {"table": T, "id": I}           -> null
 *   "account" {}                              -> "<account uuid>" | null
 *   "txBegin"    {"serializable"?: true}      -> {"tx": "<id>"}
 *   "txCommit"   {"tx", "tier"?}              -> null
 *   "txRollback" {"tx"}                       -> null
 *   "editText" {"table", "id", "column", "editedFrom", "text"} -> null
 *   "textEdit" {"table", "id", "column", "units"?: "utf8"|"utf16",
 *               "edits": [{"pos", "delete", "insert"}]}       -> null
 *   "edit"     {"table", "id", "patch"?, "edits": [...]}      -> null
 *              (edits: text / splice / jsonSet, see the README)
 *   "can"      {"table", "op": "read"|"insert"|"update"|"delete",
 *               "id"?, "row"?}                               -> true|false|null
 *              (advisory: true does not promise the write succeeds; null =
 *              unknown, e.g. no server; not available inside a transaction)
 *   "upsert"   {"table", "id", "row"}            -> the row (a new id: row has
 *              every required column; an existing row: the columns it names)
 *   "restore"  {"table", "id", "patch"?}         -> the row (patch: a whole row)
 *   "uploadBegin"  {"table", "column", "kind"?, "id"?, "row"?} -> {"upload"}
 *   "uploadPush"   {"upload", "bytes": base64}                -> null
 *   "uploadFinish" {"upload", "tier"?}                        -> the row
 *   "uploadAbort"  {"upload"}                                 -> null
 *   "session"  {}  -> {"status": "signedIn"|"expired"|"signedOut", "user"?}
 *                     | null (a local app or a backend)
 *   "syncStatus" {} -> {"link": ...}
 *   "refresh"  {}  -> null (asks the token callback for a fresh token)
 *   "link"     {"bearer"} -> the user (links a device account to the app's
 *                            own sign-in; the session becomes that user's)
 *   "signOut"  {}  -> null (signs out and closes the app)
 *   "bundle"   {}  -> "<published permissions bundle>" | null (a backend's)
 *   "waitForSync" {"tier"}                    -> null, once every write so
 *                                                far reaches "local"|"global"
 * Reads (all, one) and writes (insert, update, delete) also take "tx": run
 * inside that open transaction (its reads see its own writes), and "tier":
 * for reads "local" | "localFirst" (default) | "remote"; for writes and
 * txCommit "local" | "global", the durability to wait for. A write inside a
 * transaction takes no tier: give it to txCommit. Use a transaction from
 * one thread at a time. A transaction the host never ends stays open (and
 * holds its snapshot) until jazz_close, which rolls it back. A failed
 * txCommit ends the transaction. "global" on an app without a server is
 * refused before anything is written. TS's "local-first" is accepted for
 * "localFirst".
 */
char *jazz_call(JazzHandle *handle, const char *method, const char *args_json, char **err);

/*
 * Subscribes to query_json (a query, or {"query": Q, "tier"?}); cb gets its
 * deltas until jazz_unsubscribe.
 */
JazzSubscription *jazz_subscribe(JazzHandle *handle, const char *query_json, JazzCallback cb,
                                 void *ctx, char **err);

/*
 * Ends and frees a subscription. When it returns, no callback is running
 * or will start, so cb and ctx may be freed. Called from inside the
 * callback, it returns without waiting for that callback to finish. A
 * subscription owns what it needs: free it before or after jazz_close of
 * its handle. Because it waits, don't call jazz_unsubscribe while holding a
 * lock the callback takes, or from another subscription's callback that
 * this one may also unsubscribe: either deadlocks.
 */
void jazz_unsubscribe(JazzSubscription *subscription);

/*
 * Closes the app and frees the handle. Each subscription still open gets a
 * last "error" callback, {"kind":"closed",...}; free them with
 * jazz_unsubscribe, before or after this call.
 */
void jazz_close(JazzHandle *handle);

/*
 * An end user's token callback: called on a thread of its own (it may
 * block) with reason "connect" | "renew" | "expired" | "refresh". Return 0
 * with *token set to the token as a malloc'ed string (Go: C.CString), which
 * jazz frees, or to NULL when no one is signed in (the app stops). Return
 * non-zero when the fetch failed (e.g. a network blip): the session stays
 * signed in and asks again; *token may hold the failure's message.
 */
typedef int (*JazzTokenCallback)(void *ctx, const char *reason, char **token);

/*
 * Opens an app as an end user signing in with the app's bearer
 * ("auth": {"bearer": true}); cb and ctx stay valid, from any thread, until
 * jazz_close (or a "signOut" call) returns: that waits for a fetch in
 * progress, and then the callback is never called again. Called from inside
 * the callback, jazz_close returns at once and the callback stops once that
 * call returns. ("auth": {"deviceKey": ...} opens with jazz_open.)
 */
JazzHandle *jazz_open_user(const char *config_json, JazzTokenCallback cb, void *ctx,
                           char **err);

typedef struct JazzWatch JazzWatch;

/*
 * Calls cb with kind "session" or "sync" and each state's JSON, the
 * current one first, on a thread of its own, until jazz_unwatch.
 */
JazzWatch *jazz_watch(JazzHandle *handle, const char *what, JazzCallback cb, void *ctx,
                      char **err);

/* Ends and frees a watch: when it returns no callback is running. */
void jazz_unwatch(JazzWatch *watch);

/* A new device account's key ("jazz-auth-v1:..."); keep it as a password. */
char *jazz_generate_device_key(char **err);

/* Frees a string returned by, or stored in *err by, this API. */
void jazz_string_free(char *text);

#ifdef __cplusplus
}
#endif

#endif /* JAZZ_FFI_H */
