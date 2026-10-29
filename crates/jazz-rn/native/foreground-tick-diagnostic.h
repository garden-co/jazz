#ifndef JAZZ_RN_FOREGROUND_TICK_DIAGNOSTIC_H
#define JAZZ_RN_FOREGROUND_TICK_DIAGNOSTIC_H

#include "jazz_native_relay.h"

namespace jazz::rn {

inline const char *foregroundTickFailureMessage(
    jazz_native_relay_status status) noexcept {
  if (status != JAZZ_NATIVE_RELAY_LIFECYCLE_FAILURE) return nullptr;
  return "Jazz native foreground runtime failed during tick: local foreground processing failed";
}

}  // namespace jazz::rn

#endif
