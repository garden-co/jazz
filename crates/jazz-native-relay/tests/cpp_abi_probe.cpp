#include <cstdint>

#include "jazz_native_relay.h"

int main() {
  if (jazz_native_relay_abi_version() != 3) return 1;
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
