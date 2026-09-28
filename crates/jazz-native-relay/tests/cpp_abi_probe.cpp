#include <cstring>
#include <cstdint>

#include "jazz_native_relay.h"
#include "foreground-tick-diagnostic.h"

int main() {
  if (jazz_native_relay_abi_version() != JAZZ_NATIVE_RELAY_ABI_V2) return 1;
  const char *upstream = jazz::rn::foregroundTickFailureMessage(
      JAZZ_NATIVE_RELAY_LIFECYCLE_FAILURE,
      JAZZ_NATIVE_RELAY_TICK_DIAGNOSTIC_UPSTREAM_TERMINAL);
  if (upstream == nullptr ||
      std::strcmp(upstream,
                  "Jazz native foreground runtime failed during tick: upstream relay connection terminated; check the network and server logs for the underlying cause") !=
          0) return 2;
  const char *local = jazz::rn::foregroundTickFailureMessage(
      JAZZ_NATIVE_RELAY_LIFECYCLE_FAILURE,
      JAZZ_NATIVE_RELAY_TICK_DIAGNOSTIC_LOCAL_TICK_FAILURE);
  if (local == nullptr ||
      std::strcmp(local,
                  "Jazz native foreground runtime failed during tick: local foreground processing failed") !=
          0) return 2;
  if (jazz::rn::foregroundTickFailureMessage(
          JAZZ_NATIVE_RELAY_BACKPRESSURE,
          JAZZ_NATIVE_RELAY_TICK_DIAGNOSTIC_UPSTREAM_TERMINAL) != nullptr ||
      jazz::rn::foregroundTickFailureMessage(
          JAZZ_NATIVE_RELAY_LIFECYCLE_FAILURE,
          JAZZ_NATIVE_RELAY_TICK_DIAGNOSTIC_NONE) != nullptr ||
      jazz::rn::foregroundTickFailureMessage(
          JAZZ_NATIVE_RELAY_LIFECYCLE_FAILURE, UINT32_MAX) != nullptr) return 2;
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
