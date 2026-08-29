#ifndef JAZZ_NATIVE_RELAY_H
#define JAZZ_NATIVE_RELAY_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Return the shared native relay ABI version embedded in this artifact.
 *
 * JNI and other platform wrappers must compare this before decoding or sending
 * a command. This header intentionally exposes no database/query handles.
 */
uint16_t jazz_native_relay_abi_version(void);

typedef struct jazz_native_relay_bytes {
  uint8_t *data;
  size_t len;
} jazz_native_relay_bytes;

typedef enum jazz_native_relay_status {
  JAZZ_NATIVE_RELAY_OK = 0,
  JAZZ_NATIVE_RELAY_INVALID_ARGUMENT = 1,
  JAZZ_NATIVE_RELAY_INVALID_COMMAND = 2,
  JAZZ_NATIVE_RELAY_ENCODE_FAILURE = 3,
  JAZZ_NATIVE_RELAY_INVALID_HANDLE = 4,
  JAZZ_NATIVE_RELAY_LIFECYCLE_FAILURE = 5,
  JAZZ_NATIVE_RELAY_INVALID_ABI_RANGE = 6,
  JAZZ_NATIVE_RELAY_INCOMPATIBLE_ABI = 7,
  JAZZ_NATIVE_RELAY_BACKPRESSURE = 8,
} jazz_native_relay_status;

typedef struct jazz_native_relay_host jazz_native_relay_host;
typedef struct jazz_native_relay_host_lease jazz_native_relay_host_lease;
typedef void (*jazz_native_relay_foreground_wake_callback)(void *context, uint64_t foreground, uint8_t wake_kind, uint64_t delay_ms);
jazz_native_relay_host *jazz_native_relay_host_new(void);
void jazz_native_relay_host_free(jazz_native_relay_host *host);
/* Retain host state for one non-zero platform-issued JSI runtime token. The
 * token scopes invalidation: destroying one bridge must never retire a
 * sibling bridge's foreground aliases. */
jazz_native_relay_host_lease *jazz_native_relay_host_retain(
    jazz_native_relay_host *host,
    uint64_t runtime_token);
void jazz_native_relay_host_lease_free(jazz_native_relay_host_lease *lease);
/* Retire every foreground/client alias opened by this runtime without a clean
 * handoff. This is idempotent; late JS finalizers then observe closed handles. */
jazz_native_relay_status jazz_native_relay_host_lease_invalidate_foreground_runtime(
    jazz_native_relay_host_lease *lease);
jazz_native_relay_status jazz_native_relay_host_execute(
    jazz_native_relay_host *host,
    const uint8_t *request,
    size_t request_len,
    jazz_native_relay_bytes *out);

/* Admit strict JSON from trusted Kotlin/Swift platform code. This is not a
 * JavaScript command: unknown fields, malformed config, SYSTEM identity, and
 * bearer-token claims are rejected by Rust. On success `out` is exactly the
 * opaque 32-byte admission capability, never a config or claim echo. */
jazz_native_relay_status jazz_native_relay_host_admit_scope_json(
    jazz_native_relay_host *host,
    const uint8_t *request,
    size_t request_len,
    jazz_native_relay_bytes *out);

/* Revoke a raw opaque 32-byte admission capability held by trusted platform
 * lifecycle code. This is intentionally not exposed through execute. */
jazz_native_relay_status jazz_native_relay_host_revoke_scope_capability(
    jazz_native_relay_host *host,
    const uint8_t *capability,
    size_t capability_len);

/* Open one opaque, memory-only foreground engine from a capability admitted
 * by trusted platform code. This C ABI is consumed by the private JSI factory,
 * never by application JavaScript: it accepts no SQLite path, schema, token,
 * claims, or identity. The host copies exactly 32 bytes before queuing work on
 * the bounded native relay owner thread. */
jazz_native_relay_status jazz_native_relay_host_open_attached_foreground(
    jazz_native_relay_host *host,
    const uint8_t *capability,
    size_t capability_len,
    uint64_t *out_foreground);

/* Run one bounded ordinary core tick for an opaque foreground engine. This is
 * the first real NativeDb method exposed by the foreground host. It performs
 * no direct SQLite read: peer traffic stays on the ordinary foreground ↔ relay
 * connection. */
jazz_native_relay_status jazz_native_relay_host_tick_attached_foreground(
    jazz_native_relay_host *host,
    uint64_t foreground);

/* Close one foreground engine. This is idempotent; out_closed is true only for
 * the call that transitioned a live alias to closed. */
jazz_native_relay_status jazz_native_relay_host_close_attached_foreground(
    jazz_native_relay_host *host,
    uint64_t foreground,
    bool *out_closed);

/* Lease-only attached-foreground operations. Private JSI code must call these
 * instead of retaining a raw jazz_native_relay_host pointer across a bridge or
 * activity teardown. Every handle-taking operation rejects a foreground that
 * was opened by a different lease runtime token. */
jazz_native_relay_status jazz_native_relay_host_lease_open_attached_foreground(
    jazz_native_relay_host_lease *lease,
    const uint8_t *capability,
    size_t capability_len,
    uint64_t *out_foreground);
jazz_native_relay_status jazz_native_relay_host_lease_tick_attached_foreground(
    jazz_native_relay_host_lease *lease,
    uint64_t foreground);
jazz_native_relay_status jazz_native_relay_host_lease_close_attached_foreground(
    jazz_native_relay_host_lease *lease,
    uint64_t foreground,
    bool *out_closed);
jazz_native_relay_status jazz_native_relay_host_lease_set_foreground_wake_callback(
    jazz_native_relay_host_lease *lease,
    uint64_t foreground,
    jazz_native_relay_foreground_wake_callback callback,
    void *context);

/* Execute one complete postcard foreground NativeDb command against a live
 * attached foreground handle. This is private to native binding adapters: it
 * is not part of the public relay TurboModule command channel. ABI 7 includes
 * the existing core mergeable/exclusive transaction semantics and full-cell
 * mutations, using the shared encoded-cell record envelope. Transaction
 * handles are opaque and scoped to this foreground; commits return the public
 * 16-byte txId in the postcard response. Requests are bounded to 1 MiB. The
 * request and response vocabulary is versioned by
 * jazz_native_relay_abi_version(); result bytes are Rust-owned and released
 * with jazz_native_relay_bytes_free. */
jazz_native_relay_status jazz_native_relay_host_lease_execute_foreground(
    jazz_native_relay_host_lease *lease,
    uint64_t foreground,
    const uint8_t *request,
    size_t request_len,
    jazz_native_relay_bytes *out);

/*
 * Execute one complete postcard RelayCommandRequest. On success, response
 * bytes are Rust-owned and must be released with jazz_native_relay_bytes_free.
 * On error, out is reset to {NULL, 0}. Passing NULL for out is invalid. Before
 * every call, out must already be empty or have been freed; resetting an owned
 * buffer would otherwise discard its only pointer.
 */
jazz_native_relay_status jazz_native_relay_execute(
    const uint8_t *request,
    size_t request_len,
    jazz_native_relay_bytes *out);

/* Free one returned response and reset it to {NULL, 0}. Repeating this call on
 * the same struct is safe; copying the struct before freeing is not. */
void jazz_native_relay_bytes_free(jazz_native_relay_bytes *bytes);

#ifdef __cplusplus
}
#endif

#endif
