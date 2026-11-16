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
 * (a serializable transaction lost to another write; retry it),
 * "noAccount" (jazz_for_user: the user has no account yet; with "userId"),
 * "replaced" (a session's handle after its user changed), "noUser" (a
 * session has no user; with "status")
 * and "unsynced" (jazz_close_within: writes left unsent; with "pending"))
 * (*err is set to NULL on success). Functions block until done. Calls on
 * one handle may come from any threads at once, except jazz_close: no call
 * on that handle may run concurrently with it or follow it. Likewise no
 * call may use a subscription concurrently with or after its
 * jazz_unsubscribe.
 */
#ifndef JAZZ_FFI_H
#define JAZZ_FFI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct JazzHandle JazzHandle;
typedef struct JazzSubscription JazzSubscription;

/*
 * Called on a thread of the subscription's own. kind is "delta" (json is
 * the delta: {"added": [row], "updated": [row], "removed": [id],
 * "removedKeys": [key], "reset", "coalesced"}; each row carries "$key",
 * its result's opaque key, which a host holding history or union results
 * (where one id is several results) applies updates and removals by;
 * the first has "reset": true and the full result) or
 * "error" (json is the error JSON; no callback follows). Kind "closed"
 * means this subscription ended: its app closed, or its stream ended for
 * another reason (the client stopping, the server ending the query). Both strings are
 * valid only during the call. ctx is passed through unchanged and must be
 * usable from that thread.
 */
typedef void (*JazzCallback)(void *ctx, const char *kind, const char *json);

/*
 * Opens an app. config_json: { "app": "<jazz.app.json text>", "appId"?,
 * "serverUrl"?, "auth"?: {"deviceKey": "jazz-auth-v1:..."}, "dataDir"?,
 * "storage"?: "memory"|"persistent", "contract"?: 2 }. Without "auth" it
 * opens as a local client; with a device key, as that end user syncing
 * through the app's /jazz. Bearer users and backend clients open with
 * jazz_open_user.
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
 *   "backendClient" {}  -> "trusted" | "scoped" | null (not a backend
 *                          client; a jazz_for_user handle: its parent's)
 *   "txBegin"    {"serializable"?: true}      -> {"tx": "<id>"}
 *              ("serializable": false begins a mergeable transaction, TS's
 *              default: its writes merge with concurrent ones, and its
 *              commit is never refused as a conflict)
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
 *                     | null (a local app)
 *   "syncStatus" {} -> {"link": ...}
 *   "refresh"  {}  -> null (asks the token callback for a fresh token)
 *   "link"     {"bearer"} -> the user (links a device account to the app's
 *                            own sign-in; the session becomes that user's)
 *   "signOut"  {}  -> null (signs out and closes the app)
 *   "disconnect" {} -> null (leaves the server; local reads and writes go
 *                     on; TS's disconnect)
 *   "reconnect"  {} -> null (resumes sync after disconnect)
 *   "deleteClientStorage" {} -> null (closes the app and deletes its local
 *                     store; the next open starts empty. Refused while
 *                     another open app holds the same dataDir. Free the
 *                     handle with jazz_close after, as after signOut)
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
 *
 * A backend client (a machine syncing through the app's main backend, as
 * TS's createJazzClient with a credential) opens here with
 * "auth": {"credential": true}, "serverUrl": the main backend's /jazz, and
 * "permissions" (required): the app's jazz.permissions.json, not published
 * but how the client checks the users it acts for. cb gives the machine's
 * own token (its workload's OIDC token), fresh for each request and
 * socket. The main backend decides what it is: "trusted" (acting as the
 * system, with jazz_for_user) or "scoped" (acting as one user); a token it
 * trusts neither way fails the open with kind "refused".
 */
JazzHandle *jazz_open_user(const char *config_json, JazzTokenCallback cb, void *ctx,
                           char **err);

typedef struct JazzWatch JazzWatch;

/*
 * Calls cb with kind "session" or "sync" and each state's JSON, the
 * current one first, on a thread of its own, until jazz_unwatch. With
 * what "mutationErrors", cb gets kind "mutationErrors" and each write the
 * server refused that no durable wait reported (TS's onMutationError):
 * {"transaction", "message", "denied", "serializable"}; refusals from
 * before the first such watch come first. When the app closes (jazz_close,
 * signOut, deleteClientStorage), cb gets a last call with kind "error" and
 * {"kind":"closed",...}, after which no callback comes; free the watch
 * with jazz_unwatch, before or after.
 */
JazzWatch *jazz_watch(JazzHandle *handle, const char *what, JazzCallback cb, void *ctx,
                      char **err);

/* Ends and frees a watch: when it returns no callback is running. */
void jazz_unwatch(JazzWatch *watch);

/* A new device account's key ("jazz-auth-v1:..."); keep it as a password. */
char *jazz_generate_device_key(char **err);

/*
 * On a trusted backend client, a new handle acting as user_id (the id the
 * main backend's authenticate names the user by), with claims_json (a JSON
 * object, or NULL for {}) as the claims its permissions see. It never
 * creates an account: kind "noAccount" when the user has none yet,
 * "permission" when the main backend refuses the identity (its 403),
 * "refused" on a scoped client or a jazz_for_user handle. Free it with
 * jazz_close, which leaves handle open; once handle closes, its calls fail
 * with kind "closed". Its writes are drained by handle's jazz_close_within.
 */
JazzHandle *jazz_for_user(const JazzHandle *handle, const char *user_id,
                          const char *claims_json, char **err);

/*
 * jazz_close, first waiting up to timeout_ms for the writes the client
 * holds (every jazz_for_user handle's too) to reach the server: TS's
 * close({ timeoutMs }), 10000 by default there. Returns 0, or -1 with *err
 * set: kind "unsynced" with "pending" when writes are left unsent (memory
 * storage, where they are lost), or another failure. The handle is closed
 * and freed either way; timeout_ms 0 closes at once. The token callback
 * keeps answering while it drains.
 */
int jazz_close_within(JazzHandle *handle, uint64_t timeout_ms, char **err);

/*
 * A user-following session (TS's JazzSession): who is signed in (the app's
 * bearer, a device account, or no one) and a client for that user,
 * replaced when the user changes. config_json is the app's config with
 * "serverUrl" (the app's /jazz), "appId", "storage"/"dataDir" (each
 * account's store under dataDir/<account>/, the device key in
 * dataDir/device.key), "auth"?: {"bearer": true} (token_cb gives the
 * bearer; NULL without), "initial"?: "backend" (follow the bearer; the
 * default, needs one) | "device" | "signed-out", "contract"?: 2.
 */
typedef struct JazzSession JazzSession;
JazzSession *jazz_session_open(const char *config_json, JazzTokenCallback token_cb, void *ctx,
                               char **err);

/*
 * Calls a session's method with args_json; returns its result as JSON:
 *   "snapshot" {} -> {"status": "starting"|"ready"|"signed-out"|
 *                     "transitioning"|"error"|"closed", "user"?,
 *                     "clientEpoch", "error"?: {"kind", "message"},
 *                     "recovery"?: "action"|"session"}
 *   "refresh" {}  -> null (the app's auth changed: ask the bearer, follow)
 *   "createDeviceAccount" {} | "reopenDeviceAccount" {}
 *   | "restoreDeviceAccount" {"deviceKey"} | "linkDeviceAccount" {}
 *   | "signOut" {} (the session stays, signed out) | "retry" {} -> null
 *   "exportDeviceKey" {} -> "jazz-auth-v1:..."
 */
char *jazz_session_call(JazzSession *session, const char *method, const char *args_json,
                        char **err);

/*
 * A handle acting for the user signed in now; kind "noUser" (with
 * "status") while there is none. Free it with jazz_close, which leaves the
 * session open. Once the user changes, its calls fail with kind
 * "replaced" ("closed" without contract 2): fetch the new one when the
 * snapshot's "clientEpoch" changes.
 */
JazzHandle *jazz_session_jazz(JazzSession *session, char **err);

/*
 * Calls cb with kind "session" and each snapshot's JSON, the current one
 * first, on a thread of its own, until jazz_unwatch; when the session
 * closes, a last call with kind "error" and {"kind":"closed",...}.
 */
JazzWatch *jazz_session_watch(JazzSession *session, JazzCallback cb, void *ctx, char **err);

/*
 * Closes and frees the session: its handles fail with kind "closed" (free
 * them with jazz_close, before or after), its watches get their last call,
 * and the token callback never runs once this returns. No other call on
 * the session may run concurrently with or follow it.
 */
void jazz_session_close(JazzSession *session);

/* Frees a string returned by, or stored in *err by, this API. */
void jazz_string_free(char *text);

#ifdef __cplusplus
}
#endif

#endif /* JAZZ_FFI_H */
