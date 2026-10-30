#include <cstdint>
#include <cstring>
#include <initializer_list>

#include "jazz_native_relay.h"
#include "foreground-tick-diagnostic.h"

int main() {
  if (jazz_native_relay_abi_version() != JAZZ_NATIVE_RELAY_ABI_VERSION) return 1;
  const char *local = jazz::rn::foregroundTickFailureMessage(
      JAZZ_NATIVE_RELAY_LIFECYCLE_FAILURE);
  if (local == nullptr ||
      std::strcmp(local,
                  "Jazz native foreground runtime failed during tick: local foreground processing failed") !=
          0) return 2;
  // Other statuses keep their existing, more specific host diagnostics.
  for (const auto status : {JAZZ_NATIVE_RELAY_OK, JAZZ_NATIVE_RELAY_BACKPRESSURE,
                            JAZZ_NATIVE_RELAY_INVALID_HANDLE,
                            JAZZ_NATIVE_RELAY_INVALID_ARGUMENT}) {
    if (jazz::rn::foregroundTickFailureMessage(status) != nullptr) return 2;
  }
  jazz_native_relay_host *host = jazz_native_relay_host_new();
  if (host == nullptr) return 1;
  const std::uint8_t probe[] = {0};
  jazz_native_relay_bytes output{};
  if (jazz_native_relay_host_execute(host, probe, sizeof(probe), &output) !=
      JAZZ_NATIVE_RELAY_OK) return 1;
  jazz_native_relay_bytes_free(&output);
  jazz_native_relay_host_free(host);
  return output.data == nullptr && output.len == 0 ? 0 : 1;
}
