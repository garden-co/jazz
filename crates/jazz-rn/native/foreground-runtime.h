#ifndef JAZZ_RN_FOREGROUND_RUNTIME_H
#define JAZZ_RN_FOREGROUND_RUNTIME_H

#include <memory>
#include <mutex>
#include <vector>

#include <ReactCommon/CallInvoker.h>
#include <jsi/jsi.h>

#include "jazz_native_relay.h"

namespace jazz::rn {

class ForegroundWakeRegistration;

/** A platform-retained liveness lease shared by the factory and every opened
 * foreground HostObject. Invalidation marks handles dead before the platform
 * releases its relay-host pointer, so a late JS finalizer cannot dereference
 * freed Rust state after an activity/bridge reload. The opaque Rust lease owns
 * host state; the mutex makes invalidation mutually exclusive with foreground
 * FFI operations. */
class ForegroundRuntimeLease {
 public:
  ForegroundRuntimeLease(
      jazz_native_relay_host *host,
      uint64_t runtime_token,
      std::shared_ptr<facebook::react::CallInvoker> callInvoker)
      : lease_(jazz_native_relay_host_retain(host, runtime_token)),
        callInvoker_(std::move(callInvoker)) {}
  ~ForegroundRuntimeLease();

  /** Hold the lifecycle lock through one FFI call. An empty lock means the
   * platform invalidated this JS runtime first. */
  std::unique_lock<std::mutex> lockIfActive();
  jazz_native_relay_host_lease *nativeLease() const { return lease_; }
  const std::shared_ptr<facebook::react::CallInvoker> &callInvoker() const {
    return callInvoker_;
  }
  bool active() const;
  void invalidate();

  /** Internal ownership registry. Every foreground wake registration is
   * invalidated and synchronously detached from Rust before the retained lease
   * can be released, preventing a late owner-thread callback from observing a
   * destroyed JSI object. */
  void trackWakeRegistration(
      const std::shared_ptr<ForegroundWakeRegistration> &registration);

 private:
  jazz_native_relay_host_lease *lease_;
  std::shared_ptr<facebook::react::CallInvoker> callInvoker_;
  mutable std::mutex mutex_;
  bool active_{true};
  std::vector<std::weak_ptr<ForegroundWakeRegistration>> wakeRegistrations_;
};

/**
 * Install the private versioned foreground factory into exactly one JS runtime.
 *
 * The platform module calls this only while React Native has handed it the live
 * JSI runtime. `host` remains platform-owned: the platform must retain its
 * relay-host lease until every factory and foreground HostObject from this
 * runtime has been invalidated. The installed HostObjects never expose that
 * pointer to JavaScript and copy the 32 capability bytes before invoking the
 * C ABI.
 */
void installForegroundRuntime(
    facebook::jsi::Runtime &runtime,
    const std::shared_ptr<ForegroundRuntimeLease> &lease);

}  // namespace jazz::rn

#endif  // JAZZ_RN_FOREGROUND_RUNTIME_H
