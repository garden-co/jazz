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
 * (*err is set to NULL on success). Every function is safe to call from any
 * thread and blocks until done.
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
 * "error" (json is the error JSON; no callback follows). Both strings are
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
 */
char *jazz_call(JazzHandle *handle, const char *method, const char *args_json, char **err);

/* Subscribes to query_json; cb gets its deltas until jazz_unsubscribe. */
JazzSubscription *jazz_subscribe(JazzHandle *handle, const char *query_json, JazzCallback cb,
                                 void *ctx, char **err);

/* Ends and frees a subscription: no callback starts after it returns. */
void jazz_unsubscribe(JazzSubscription *subscription);

/*
 * Closes the app and frees the handle. Open subscriptions end (still free
 * them with jazz_unsubscribe).
 */
void jazz_close(JazzHandle *handle);

/* Frees a string returned by, or stored in *err by, this API. */
void jazz_string_free(char *text);

#ifdef __cplusplus
}
#endif

#endif /* JAZZ_FFI_H */
