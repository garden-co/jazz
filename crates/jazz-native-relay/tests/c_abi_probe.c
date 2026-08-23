#include <stdint.h>
#include <stdio.h>

#include "jazz_native_relay.h"

int main(void) {
  const uint16_t abi = jazz_native_relay_abi_version();
  if (abi != 1) {
    fprintf(stderr, "unexpected Jazz native relay ABI: %u\n", abi);
    return 1;
  }
  return 0;
}
