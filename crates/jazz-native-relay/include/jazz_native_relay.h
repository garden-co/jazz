#ifndef JAZZ_NATIVE_RELAY_H
#define JAZZ_NATIVE_RELAY_H

#include <stdint.h>
#include <stddef.h>

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
jazz_native_relay_host *jazz_native_relay_host_new(void);
void jazz_native_relay_host_free(jazz_native_relay_host *host);
jazz_native_relay_status jazz_native_relay_host_execute(
    jazz_native_relay_host *host,
    const uint8_t *request,
    size_t request_len,
    jazz_native_relay_bytes *out);

/* Admit a postcard RelayScopeAdmissionRequest supplied by trusted platform
 * code. JavaScript receives only the resulting opaque random capability. */
jazz_native_relay_status jazz_native_relay_host_admit_scope(
    jazz_native_relay_host *host,
    const uint8_t *request,
    size_t request_len,
    jazz_native_relay_bytes *out);

/* Revoke a postcard RelayScopeRevocationRequest from trusted platform code.
 * This closes every relay and UI-client alias opened by that capability. */
jazz_native_relay_status jazz_native_relay_host_revoke_scope(
    jazz_native_relay_host *host,
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
