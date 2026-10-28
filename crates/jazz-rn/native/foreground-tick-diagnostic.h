#ifndef JAZZ_RN_FOREGROUND_TICK_DIAGNOSTIC_H
#define JAZZ_RN_FOREGROUND_TICK_DIAGNOSTIC_H

#include <cstdint>

#include "jazz_native_relay.h"

namespace jazz::rn {

inline const char *foregroundTickFailureMessage(
    jazz_native_relay_status status, uint32_t diagnostic) noexcept {
  if (status != JAZZ_NATIVE_RELAY_LIFECYCLE_FAILURE) return nullptr;
  switch (diagnostic) {
    case JAZZ_NATIVE_RELAY_TICK_DIAGNOSTIC_LOCAL_TICK_FAILURE:
      return "Jazz native foreground runtime failed during tick: local foreground processing failed";
    default:
      return nullptr;
  }
}

}  // namespace jazz::rn

#endif
