#include <stdint.h>
#include <stdio.h>

#include "jazz_native_relay.h"

int main(void) {
  const uint16_t abi = jazz_native_relay_abi_version();
  if (abi != 3) {
    fprintf(stderr, "unexpected Jazz native relay ABI: %u\n", abi);
    return 1;
  }
  /* RelayCommandRequest::Probe's postcard discriminant. The response is
   * intentionally opaque to JNI/C; only the shared Rust/JS codec decodes it. */
  const uint8_t probe[] = {0};
  jazz_native_relay_bytes response = {0};
  if (jazz_native_relay_execute(probe, sizeof(probe), &response) != JAZZ_NATIVE_RELAY_OK ||
      response.data == NULL || response.len == 0) {
    fprintf(stderr, "relay probe command did not return an owned response\n");
    return 1;
  }
  jazz_native_relay_bytes_free(&response);
  if (response.data != NULL || response.len != 0) {
    fprintf(stderr, "relay response free did not reset ownership\n");
    return 1;
  }
  jazz_native_relay_bytes_free(&response);
  jazz_native_relay_host *host = jazz_native_relay_host_new();
  if (host == NULL || jazz_native_relay_host_execute(host, probe, sizeof(probe), &response) !=
      JAZZ_NATIVE_RELAY_OK) {
    fprintf(stderr, "host-owned relay probe failed\n");
    return 1;
  }
  jazz_native_relay_bytes_free(&response);
  response.data = (uint8_t *)(uintptr_t)1;
  response.len = 17;
  if (jazz_native_relay_host_execute(NULL, probe, sizeof(probe), &response) !=
          JAZZ_NATIVE_RELAY_INVALID_ARGUMENT ||
      response.data != NULL || response.len != 0) {
    fprintf(stderr, "invalid host did not reset output\n");
    return 1;
  }
  /* Trusted scope admission is a dedicated ABI, not an execute command. An
   * invalid JSON fixture proves the symbol is linked without placing config or
   * credentials on the generic command channel. */
  const uint8_t invalid_admission[] = {'{'};
  if (jazz_native_relay_host_admit_scope_json(host, invalid_admission,
                                               sizeof(invalid_admission), &response) !=
          JAZZ_NATIVE_RELAY_INVALID_COMMAND ||
      response.data != NULL || response.len != 0) {
    fprintf(stderr, "trusted admission JSON did not fail closed\n");
    return 1;
  }
  uint8_t capability[32] = {0};
  if (jazz_native_relay_host_revoke_scope_capability(host, capability,
                                                      sizeof(capability)) !=
      JAZZ_NATIVE_RELAY_OK) {
    fprintf(stderr, "trusted capability revocation ABI failed\n");
    return 1;
  }
  jazz_native_relay_host_free(host);
  if (jazz_native_relay_execute((const uint8_t *)"\xff", 1, &response) !=
      JAZZ_NATIVE_RELAY_INVALID_COMMAND) {
    fprintf(stderr, "relay invalid command did not report a typed error\n");
    return 1;
  }
  if (response.data != NULL || response.len != 0) {
    fprintf(stderr, "relay invalid command retained output ownership\n");
    return 1;
  }
  return 0;
}
