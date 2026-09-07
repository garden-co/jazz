#include "foreground-runtime.h"
#include "account-store-lock.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifdef __ANDROID__
#include <android/log.h>
#endif

namespace jazz::rn {

void traceForegroundWake(const char *stage) noexcept {
#ifdef __ANDROID__
  __android_log_print(ANDROID_LOG_ERROR, "JazzForegroundWake", "%s", stage);
#else
  (void)stage;
#endif
}

constexpr const char *kWakeCallbacksGlobal = "__jazzNativeForegroundWakeCallbacksV1";
constexpr uint8_t kWakeImmediate = 0;
constexpr uint8_t kWakeDeferred = 1;
constexpr uint8_t kWakeAfter = 2;
constexpr uint8_t kWakeCancelled = 3;

using facebook::jsi::Function;
using facebook::jsi::JSError;
using facebook::jsi::Object;
using facebook::jsi::Runtime;
using facebook::jsi::String;
using facebook::jsi::Value;

/**
 * One platform-scheduled wake sink for one foreground Db in one JS runtime.
 *
 * Rust may signal this object from its owner thread. The trampoline retains no
 * JavaScript values and only queues a CallInvoker task. That task reacquires
 * the callback from a runtime-local private object and invokes it after every
 * host/relay lock has been released. A pending bit coalesces bursts; a wake
 * that arrives while JS is delivering one callback schedules one later turn,
 * never reenters JavaScript synchronously.
 */
class ForegroundWakeRegistration final
    : public std::enable_shared_from_this<ForegroundWakeRegistration> {
 public:
  ForegroundWakeRegistration(
      uint64_t foreground,
      std::shared_ptr<facebook::react::CallInvoker> callInvoker)
      : foreground_(foreground), callInvoker_(std::move(callInvoker)) {}

  void installCallback(Runtime &runtime, Function callback) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!active_) {
        throw JSError(runtime, "Jazz native foreground runtime is unavailable after teardown");
      }
    }
    auto callbacks = runtime.global().getProperty(runtime, kWakeCallbacksGlobal);
    if (!callbacks.isObject()) {
      throw JSError(runtime, "Jazz native foreground wake registry is unavailable");
    }
    callbacks.asObject(runtime).setProperty(
        runtime, callbackKey().c_str(), Value(std::move(callback)));
  }

  void removeCallback(Runtime &runtime) {
    auto callbacks = runtime.global().getProperty(runtime, kWakeCallbacksGlobal);
    if (callbacks.isObject()) {
      callbacks.asObject(runtime).setProperty(runtime, callbackKey().c_str(), Value::undefined());
    }
  }

  jazz_native_relay_status activateNative(jazz_native_relay_host_lease *lease) {
    return jazz_native_relay_host_lease_set_foreground_wake_callback(
        lease, foreground_, &ForegroundWakeRegistration::wakeFromOwner, this);
  }

  // This is a bounded device-receipt diagnostic, not part of the foreground
  // wake contract. It is off unless the one receipt alias explicitly enables
  // it, and the same mutex that protects the coalescer makes toggling safe
  // against owner-thread wake delivery.
  void setTraceEnabled(bool enabled) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    traceEnabled_ = enabled;
  }

  /** Clear the Rust scheduler synchronously before this callback context can
   * be destroyed. This never touches JSI, so platform invalidation may call it
   * while a runtime is already being torn down. */
  void deactivateAndClear(jazz_native_relay_host_lease *lease) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      active_ = false;
      pending_ = false;
    }
    // An already-revoked foreground reports INVALID_HANDLE here; revocation
    // itself synchronously removed its Db and therefore its scheduler first.
    (void)jazz_native_relay_host_lease_set_foreground_wake_callback(
        lease, foreground_, nullptr, nullptr);
  }

 private:
  static void wakeFromOwner(void *context, uint64_t foreground, uint8_t kind,
                            uint64_t delayMs) noexcept {
    auto *registration = static_cast<ForegroundWakeRegistration *>(context);
    if (registration != nullptr) registration->requestWake(foreground, kind, delayMs);
  }

  void requestWake(uint64_t foreground, uint8_t kind, uint64_t delayMs) noexcept {
    std::shared_ptr<facebook::react::CallInvoker> invoker;
    bool traceRequested = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (foreground != foreground_) return;
      if (!active_) return;
      // Record bridge entry before coalescing can return for an already
      // scheduled wake. The fixed marker intentionally carries no identity or
      // payload data.
      traceRequested = traceEnabled_;
      if (kind == kWakeCancelled) {
        active_ = false;
        pending_ = false;
      } else {
        mergeWakeLocked(kind, delayMs);
        if (!scheduled_ && callInvoker_) {
          scheduled_ = true;
          invoker = callInvoker_;
        }
      }
    }
    if (traceRequested) traceForegroundWake("requested");
    if (!invoker) return;
    schedule(std::move(invoker));
  }

  void schedule(std::shared_ptr<facebook::react::CallInvoker> invoker) noexcept {
    try {
      auto self = shared_from_this();
      bool traceScheduled = false;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        traceScheduled = traceEnabled_;
      }
      if (traceScheduled) traceForegroundWake("scheduled");
      invoker->invokeAsync([self = std::move(self)](Runtime &runtime) {
        self->deliver(runtime);
      });
    } catch (...) {
      // Never let an executor failure unwind through Rust's C callback. Keep
      // the pending wake eligible for a later owner signal instead of leaving
      // the coalescer permanently marked as scheduled.
      std::lock_guard<std::mutex> lock(mutex_);
      scheduled_ = false;
    }
  }

  void deliver(Runtime &runtime) {
    uint8_t kind = kWakeDeferred;
    uint64_t delayMs = 0;
    bool traceDelivery = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      scheduled_ = false;
      if (!active_ || !pending_) return;
      pending_ = false;
      delivering_ = true;
      kind = kind_;
      delayMs = delayMs_;
      traceDelivery = traceEnabled_;
    }
    if (traceDelivery) traceForegroundWake("delivered");

    try {
      auto callbacks = runtime.global().getProperty(runtime, kWakeCallbacksGlobal);
      if (callbacks.isObject()) {
        auto callback = callbacks.asObject(runtime).getProperty(runtime, callbackKey().c_str());
        if (callback.isObject() && callback.asObject(runtime).isFunction(runtime)) {
          auto urgency = String::createFromUtf8(runtime, urgencyFor(kind, delayMs));
          callback.asObject(runtime).asFunction(runtime).call(runtime, Value(std::move(urgency)));
          // This marker means the JSI callback returned to the native bridge.
          if (traceDelivery) traceForegroundWake("callback-invoked");
        }
      }
    } catch (const JSError &) {
      // A scheduler callback is application-controlled. Do not let a thrown
      // JS callback unwind through CallInvoker or leave the coalescer stuck.
    } catch (...) {
      // Same guarantee for an engine-specific exception type.
    }

    std::shared_ptr<facebook::react::CallInvoker> invoker;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      delivering_ = false;
      if (active_ && pending_ && !scheduled_ && callInvoker_) {
        scheduled_ = true;
        invoker = callInvoker_;
      }
    }
    if (invoker) schedule(std::move(invoker));
  }

  void mergeWakeLocked(uint8_t kind, uint64_t delayMs) {
    if (!pending_) {
      pending_ = true;
      kind_ = kind;
      delayMs_ = delayMs;
      return;
    }
    // Service the most urgent work that arrived in this coalesced turn. An
    // explicit timer remains a timer unless a normal wake supersedes it.
    if (kind == kWakeImmediate ||
        (kind == kWakeDeferred && kind_ == kWakeAfter) ||
        (kind == kWakeAfter && kind_ == kWakeAfter && delayMs < delayMs_)) {
      kind_ = kind;
      delayMs_ = delayMs;
    }
  }

  std::string callbackKey() const { return "foreground-" + std::to_string(foreground_); }

  static std::string urgencyFor(uint8_t kind, uint64_t delayMs) {
    if (kind == kWakeImmediate) return "immediate";
    if (kind == kWakeAfter) return "after:" + std::to_string(delayMs);
    return "deferred";
  }

  uint64_t foreground_;
  std::shared_ptr<facebook::react::CallInvoker> callInvoker_;
  std::mutex mutex_;
  bool active_{true};
  bool pending_{false};
  bool scheduled_{false};
  bool delivering_{false};
  bool traceEnabled_{false};
  uint8_t kind_{kWakeDeferred};
  uint64_t delayMs_{0};
};

ForegroundRuntimeLease::~ForegroundRuntimeLease() {
  invalidate();
  if (lease_ != nullptr) {
    jazz_native_relay_host_lease_free(lease_);
    lease_ = nullptr;
  }
}

std::unique_lock<std::mutex> ForegroundRuntimeLease::lockIfActive() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!active_ || lease_ == nullptr) {
    lock.unlock();
  }
  return lock;
}

bool ForegroundRuntimeLease::active() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_ && lease_ != nullptr;
}

void ForegroundRuntimeLease::invalidate() {
  std::vector<std::shared_ptr<ForegroundWakeRegistration>> registrations;
  jazz_native_relay_host_lease *nativeLease = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!active_) return;
    active_ = false;
    nativeLease = lease_;
    for (const auto &weak : wakeRegistrations_) {
      if (auto registration = weak.lock()) registrations.push_back(std::move(registration));
    }
    wakeRegistrations_.clear();
  }
  for (const auto &registration : registrations) {
    registration->deactivateAndClear(nativeLease);
  }
  // This is an unclean runtime handoff: JS finalizers may never run. Rust
  // must retire exactly this runtime's aliases and node leases before a
  // sibling bridge can continue using the shared relay host.
  (void)jazz_native_relay_host_lease_invalidate_foreground_runtime(nativeLease);
}

void ForegroundRuntimeLease::trackWakeRegistration(
    const std::shared_ptr<ForegroundWakeRegistration> &registration) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!active_) return;
  wakeRegistrations_.erase(
      std::remove_if(wakeRegistrations_.begin(), wakeRegistrations_.end(),
                     [](const auto &weak) { return weak.expired(); }),
      wakeRegistrations_.end());
  wakeRegistrations_.push_back(registration);
}

namespace {

using facebook::jsi::ArrayBuffer;
using facebook::jsi::HostObject;
using facebook::jsi::PropNameID;

constexpr const char *kFactoryGlobal = "__jazzNativeForegroundRuntimeV1";
constexpr size_t kForegroundCommandMaxBytes = 1024 * 1024;

class VectorMutableBuffer final : public facebook::jsi::MutableBuffer {
 public:
  explicit VectorMutableBuffer(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

  size_t size() const override { return bytes_.size(); }
  uint8_t *data() override { return bytes_.data(); }

 private:
  std::vector<uint8_t> bytes_;
};

[[noreturn]] void throwStatus(Runtime &runtime, jazz_native_relay_status status,
                              const char *operation) {
  switch (status) {
    case JAZZ_NATIVE_RELAY_INVALID_HANDLE:
      throw JSError(runtime, std::string("Jazz native foreground runtime rejected ") +
                                operation + ": capability or handle is no longer admitted");
    case JAZZ_NATIVE_RELAY_BACKPRESSURE:
      throw JSError(runtime, std::string("Jazz native foreground runtime is busy during ") +
                                operation + "; retry after the next scheduled tick");
    default:
      throw JSError(runtime, std::string("Jazz native foreground runtime failed during ") +
                                operation);
  }
}

std::array<uint8_t, 32> copyAdmittedCapability(Runtime &runtime,
                                                const Value &value) {
  // JSI intentionally has no TypedArray wrapper. Verify the observable
  // Uint8Array shape, then copy only its selected ArrayBuffer window. This is
  // defence in depth: Rust admission remains authoritative, so a JS object
  // which merely imitates this shape cannot mint a usable capability.
  if (!value.isObject()) {
    throw JSError(runtime,
                 "Jazz native foreground runtime requires a Uint8Array capability");
  }
  auto typed = value.asObject(runtime);
  const auto expected_constructor = runtime.global().getProperty(runtime, "Uint8Array");
  const auto actual_constructor = typed.getProperty(runtime, "constructor");
  if (!expected_constructor.isObject() || !actual_constructor.isObject() ||
      !facebook::jsi::Object::strictEquals(
          runtime, expected_constructor.asObject(runtime), actual_constructor.asObject(runtime))) {
    throw JSError(runtime,
                 "Jazz native foreground runtime requires a Uint8Array capability");
  }
  auto bufferValue = typed.getProperty(runtime, "buffer");
  if (!bufferValue.isObject() || !bufferValue.asObject(runtime).isArrayBuffer(runtime)) {
    throw JSError(runtime,
                 "Jazz native foreground runtime requires a Uint8Array capability");
  }
  auto lengthValue = typed.getProperty(runtime, "byteLength");
  auto offsetValue = typed.getProperty(runtime, "byteOffset");
  if (!lengthValue.isNumber() || !offsetValue.isNumber()) {
    throw JSError(runtime,
                 "Jazz native foreground runtime requires a 32-byte admitted capability");
  }
  auto buffer = bufferValue.asObject(runtime).getArrayBuffer(runtime);
  const auto length = lengthValue.asNumber();
  const auto offset_number = offsetValue.asNumber();
  const auto buffer_size = buffer.size(runtime);
  if (!std::isfinite(length) || length != 32 || !std::isfinite(offset_number) ||
      offset_number < 0 || std::floor(offset_number) != offset_number ||
      offset_number > static_cast<double>(std::numeric_limits<size_t>::max()) ||
      offset_number > static_cast<double>(buffer_size)) {
    throw JSError(runtime,
                 "Jazz native foreground runtime requires a 32-byte admitted capability");
  }
  const auto offset = static_cast<size_t>(offset_number);
  if (offset > buffer.size(runtime) || 32 > buffer.size(runtime) - offset) {
    throw JSError(runtime,
                 "Jazz native foreground runtime requires a 32-byte admitted capability");
  }
  std::array<uint8_t, 32> capability{};
  std::memcpy(capability.data(), buffer.data(runtime) + offset, capability.size());
  return capability;
}

std::vector<uint8_t> copyForegroundCommand(Runtime &runtime, const Value &value) {
  if (!value.isObject()) {
    throw JSError(runtime, "Jazz native foreground command requires a Uint8Array");
  }
  auto typed = value.asObject(runtime);
  const auto expected_constructor = runtime.global().getProperty(runtime, "Uint8Array");
  const auto actual_constructor = typed.getProperty(runtime, "constructor");
  if (!expected_constructor.isObject() || !actual_constructor.isObject() ||
      !facebook::jsi::Object::strictEquals(
          runtime, expected_constructor.asObject(runtime), actual_constructor.asObject(runtime))) {
    throw JSError(runtime, "Jazz native foreground command requires a Uint8Array");
  }
  auto bufferValue = typed.getProperty(runtime, "buffer");
  auto lengthValue = typed.getProperty(runtime, "byteLength");
  auto offsetValue = typed.getProperty(runtime, "byteOffset");
  if (!bufferValue.isObject() || !bufferValue.asObject(runtime).isArrayBuffer(runtime) ||
      !lengthValue.isNumber() || !offsetValue.isNumber()) {
    throw JSError(runtime, "Jazz native foreground command requires a Uint8Array");
  }
  const auto length_number = lengthValue.asNumber();
  const auto offset_number = offsetValue.asNumber();
  auto buffer = bufferValue.asObject(runtime).getArrayBuffer(runtime);
  const auto buffer_size = buffer.size(runtime);
  if (!std::isfinite(length_number) || length_number < 0 ||
      std::floor(length_number) != length_number ||
      length_number > static_cast<double>(kForegroundCommandMaxBytes) ||
      !std::isfinite(offset_number) || offset_number < 0 ||
      std::floor(offset_number) != offset_number ||
      offset_number > static_cast<double>(std::numeric_limits<size_t>::max()) ||
      offset_number > static_cast<double>(buffer_size)) {
    throw JSError(runtime, "Jazz native foreground command is malformed or too large");
  }
  const auto length = static_cast<size_t>(length_number);
  const auto offset = static_cast<size_t>(offset_number);
  if (length > buffer_size - offset) {
    throw JSError(runtime, "Jazz native foreground command is malformed or too large");
  }
  const auto *begin = buffer.data(runtime) + offset;
  return std::vector<uint8_t>(begin, begin + length);
}

class NativeResponseOwner final {
 public:
  explicit NativeResponseOwner(jazz_native_relay_bytes *bytes) noexcept : bytes_(bytes) {}
  ~NativeResponseOwner() { jazz_native_relay_bytes_free(bytes_); }
  NativeResponseOwner(const NativeResponseOwner &) = delete;
  NativeResponseOwner &operator=(const NativeResponseOwner &) = delete;
 private:
  jazz_native_relay_bytes *bytes_;
};

Value foregroundResponse(Runtime &runtime, jazz_native_relay_bytes *response) {
  NativeResponseOwner ownership(response);
  std::vector<uint8_t> bytes;
  if (response->len != 0) {
    bytes.assign(response->data, response->data + response->len);
  }
  auto arrayBuffer = ArrayBuffer(runtime, std::make_shared<VectorMutableBuffer>(std::move(bytes)));
  auto uint8Array = runtime.global().getPropertyAsFunction(runtime, "Uint8Array");
  return uint8Array.callAsConstructor(runtime, std::move(arrayBuffer));
}

// Extracted JS methods may outlive the HostObject wrapper. Every method retains
// this shared handle, including its lease, closed state, and wake registration.
class ForegroundHandle final : public HostObject,
                               public std::enable_shared_from_this<ForegroundHandle> {
 public:
  ForegroundHandle(std::shared_ptr<ForegroundRuntimeLease> lease, uint64_t handle)
      : lease_(std::move(lease)),
        handle_(handle),
        wake_(std::make_shared<ForegroundWakeRegistration>(handle, lease_->callInvoker())) {
    lease_->trackWakeRegistration(wake_);
  }

  ~ForegroundHandle() override { closeNoThrow(); }

  Value get(Runtime &runtime, const PropNameID &name) override {
    const auto property = name.utf8(runtime);
    if (property == "isClosed") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "isClosed"), 0,
          [self = shared_from_this()](Runtime &runtime, const Value &, const Value *, size_t) {
            if (self->closed_) return Value(true);
            auto lease_lock = self->lease_->lockIfActive();
            if (!lease_lock.owns_lock()) return Value(true);
            // Probe is the canonical V1 unit command, discriminant zero.
            const uint8_t probe = 0;
            jazz_native_relay_bytes response{};
            const auto status = jazz_native_relay_host_lease_execute_foreground(
                self->lease_->nativeLease(), self->handle_, &probe, 1, &response);
            jazz_native_relay_bytes_free(&response);
            if (status == JAZZ_NATIVE_RELAY_INVALID_HANDLE) return Value(true);
            if (status != JAZZ_NATIVE_RELAY_OK) throwStatus(runtime, status, "isClosed");
            return Value(false);
          });
    }
    if (property == "tick") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "tick"), 0,
          [self = shared_from_this()](Runtime &runtime, const Value &, const Value *, size_t) {
            if (self->closed_) {
              throw JSError(runtime, "Jazz native foreground runtime is closed");
            }
            auto lease_lock = self->lease_->lockIfActive();
            if (!lease_lock.owns_lock()) {
              throw JSError(runtime, "Jazz native foreground runtime is unavailable after teardown");
            }
            const auto status = jazz_native_relay_host_lease_tick_attached_foreground(
                self->lease_->nativeLease(), self->handle_);
            if (status != JAZZ_NATIVE_RELAY_OK) {
              throwStatus(runtime, status, "tick");
            }
            return Value::undefined();
          });
    }
    if (property == "close") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "close"), 0,
          [self = shared_from_this()](Runtime &runtime, const Value &, const Value *, size_t) {
            return Value(self->close(runtime));
          });
    }
    if (property == "setTickScheduler") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "setTickScheduler"), 1,
          [self = shared_from_this()](Runtime &runtime, const Value &, const Value *args, size_t count) {
            if (self->closed_) {
              throw JSError(runtime, "Jazz native foreground runtime is closed");
            }
            if (count != 1 || !args[0].isObject() ||
                !args[0].asObject(runtime).isFunction(runtime)) {
              throw JSError(runtime,
                           "Jazz native foreground tick scheduler requires a function");
            }
            auto lease_lock = self->lease_->lockIfActive();
            if (!lease_lock.owns_lock()) {
              throw JSError(runtime, "Jazz native foreground runtime is unavailable after teardown");
            }
            auto callback = args[0].asObject(runtime).asFunction(runtime);
            self->wake_->installCallback(runtime, std::move(callback));
            const auto status = self->wake_->activateNative(self->lease_->nativeLease());
            if (status != JAZZ_NATIVE_RELAY_OK) {
              self->wake_->removeCallback(runtime);
              throwStatus(runtime, status, "setTickScheduler");
            }
            return Value::undefined();
          });
    }
    if (property == "setWakeTrace") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "setWakeTrace"), 1,
          [self = shared_from_this()](Runtime &runtime, const Value &, const Value *args, size_t count) {
            if (self->closed_) {
              throw JSError(runtime, "Jazz native foreground runtime is closed");
            }
            if (count != 1 || !args[0].isBool()) {
              throw JSError(runtime, "Jazz native foreground wake trace requires a boolean");
            }
            auto lease_lock = self->lease_->lockIfActive();
            if (!lease_lock.owns_lock()) {
              throw JSError(runtime, "Jazz native foreground runtime is unavailable after teardown");
            }
            self->wake_->setTraceEnabled(args[0].getBool());
            return Value::undefined();
          });
    }
    if (property == "execute") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "execute"), 1,
          [self = shared_from_this()](Runtime &runtime, const Value &, const Value *args, size_t count) {
            if (self->closed_) {
              throw JSError(runtime, "Jazz native foreground runtime is closed");
            }
            if (count != 1) {
              throw JSError(runtime, "Jazz native foreground command requires a Uint8Array");
            }
            auto lease_lock = self->lease_->lockIfActive();
            if (!lease_lock.owns_lock()) {
              throw JSError(runtime, "Jazz native foreground runtime is unavailable after teardown");
            }
            const auto request = copyForegroundCommand(runtime, args[0]);
            jazz_native_relay_bytes response{};
            const auto status = jazz_native_relay_host_lease_execute_foreground(
                self->lease_->nativeLease(), self->handle_, request.data(), request.size(), &response);
            if (status != JAZZ_NATIVE_RELAY_OK) {
              jazz_native_relay_bytes_free(&response);
              throwStatus(runtime, status, "execute");
            }
            return foregroundResponse(runtime, &response);
          });
    }
    return Value::undefined();
  }

  std::vector<PropNameID> getPropertyNames(Runtime &runtime) override {
    std::vector<PropNameID> names;
    names.reserve(6);
    names.emplace_back(PropNameID::forAscii(runtime, "tick"));
    names.emplace_back(PropNameID::forAscii(runtime, "isClosed"));
    names.emplace_back(PropNameID::forAscii(runtime, "close"));
    names.emplace_back(PropNameID::forAscii(runtime, "execute"));
    names.emplace_back(PropNameID::forAscii(runtime, "setTickScheduler"));
    names.emplace_back(PropNameID::forAscii(runtime, "setWakeTrace"));
    return names;
  }

 private:
  bool close(Runtime &runtime) {
    if (closed_) {
      return false;
    }
    bool closed = false;
    auto lease_lock = lease_->lockIfActive();
    if (!lease_lock.owns_lock()) {
      closed_ = true;
      return false;
    }
    wake_->deactivateAndClear(lease_->nativeLease());
    wake_->removeCallback(runtime);
    const auto status = jazz_native_relay_host_lease_close_attached_foreground(
        lease_->nativeLease(), handle_, &closed);
    if (status != JAZZ_NATIVE_RELAY_OK) {
      throwStatus(runtime, status, "close");
    }
    closed_ = true;
    return closed;
  }

  void closeNoThrow() {
    auto lease_lock = lease_->lockIfActive();
    if (!closed_ && lease_lock.owns_lock()) {
      wake_->deactivateAndClear(lease_->nativeLease());
      bool ignored = false;
      (void)jazz_native_relay_host_lease_close_attached_foreground(
          lease_->nativeLease(), handle_, &ignored);
      closed_ = true;
    }
    closed_ = true;
  }

  std::shared_ptr<ForegroundRuntimeLease> lease_;
  uint64_t handle_;
  std::shared_ptr<ForegroundWakeRegistration> wake_;
  bool closed_{false};
};

class ForegroundFactory final : public HostObject {
 public:
  explicit ForegroundFactory(std::shared_ptr<ForegroundRuntimeLease> lease)
      : lease_(std::move(lease)) {}

  Value get(Runtime &runtime, const PropNameID &name) override {
    const auto property = name.utf8(runtime);
    if (property == "abiVersion") {
      return Value(jazz_native_relay_abi_version());
    }
    if (property == "beginAccountSession" || property == "attachAccountSchema" ||
        property == "releaseAccountSession" || property == "refreshAccountSession") {
      const auto arity = (property == "attachAccountSchema" || property == "refreshAccountSession") ? 2 : 1;
      return Function::createFromHostFunction(
          runtime, PropNameID::forUtf8(runtime, property), arity,
          [lease = lease_, property, arity](Runtime &runtime, const Value &, const Value *args, size_t count) {
            if (count != static_cast<size_t>(arity)) throw JSError(runtime, "Invalid native account arguments");
            auto lock = lease->lockIfActive();
            if (!lock.owns_lock()) throw JSError(runtime, "Jazz native runtime is closed");
            jazz_native_relay_bytes output{nullptr, 0};
            jazz_native_relay_status status;
            if (property == "beginAccountSession") {
              if (!args[0].isString()) throw JSError(runtime, "Native account setup requires logical metadata");
              const auto request = args[0].asString(runtime).utf8(runtime);
              const auto &root = lease->storageRoot();
              status = jazz_native_relay_host_lease_begin_account_session_json(
                  lease->nativeLease(), reinterpret_cast<const uint8_t *>(request.data()), request.size(),
                  reinterpret_cast<const uint8_t *>(root.data()), root.size(), &output);
            } else {
              const auto capability = copyAdmittedCapability(runtime, args[0]);
              if (property == "releaseAccountSession") {
                status = jazz_native_relay_host_lease_release_account_session(
                    lease->nativeLease(), capability.data(), capability.size());
                if (status != JAZZ_NATIVE_RELAY_OK) throwStatus(runtime, status, "releaseAccountSession");
                return Value::undefined();
              }
              if (property == "refreshAccountSession") {
                if (!args[1].isString()) throw JSError(runtime, "Native account refresh requires session JSON");
                const auto request = args[1].asString(runtime).utf8(runtime);
                status = jazz_native_relay_host_lease_refresh_account_session(
                    lease->nativeLease(), capability.data(), capability.size(),
                    reinterpret_cast<const uint8_t *>(request.data()), request.size());
                if (status != JAZZ_NATIVE_RELAY_OK) throwStatus(runtime, status, "refreshAccountSession");
                return Value::undefined();
              }
              if (!args[1].isString()) throw JSError(runtime, "Native account schema must be canonical JSON");
              const auto schema = args[1].asString(runtime).utf8(runtime);
              status = jazz_native_relay_host_lease_attach_account_schema_json(
                  lease->nativeLease(), capability.data(), capability.size(),
                  reinterpret_cast<const uint8_t *>(schema.data()), schema.size(), &output);
            }
            if (status != JAZZ_NATIVE_RELAY_OK) throwStatus(runtime, status, property.c_str());
            return foregroundResponse(runtime, &output);
          });
    }
    if (property == "withAccountStoreLock") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "withAccountStoreLock"), 1,
          [lease = lease_](Runtime &runtime, const Value &, const Value *args, size_t count) {
            if (count != 1 || !args[0].isObject() || !args[0].asObject(runtime).isFunction(runtime)) {
              throw JSError(runtime, "Jazz account store update requires a synchronous callback");
            }
            if (!lease->active()) throw JSError(runtime, "Jazz native runtime is closed");
            try {
              AccountStoreLock lock(lease->storageRoot());
              if (!lease->active()) throw JSError(runtime, "Jazz native runtime is closed");
              const auto result = args[0].asObject(runtime).asFunction(runtime).call(runtime);
              if (!result.isUndefined()) {
                throw JSError(runtime, "Jazz account store callback must return undefined synchronously");
              }
              return Value::undefined();
            } catch (const std::exception &error) {
              throw JSError(runtime, error.what());
            }
          });
    }
    if (property == "accountSecret") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "accountSecret"), 0,
          [lease = lease_](Runtime &runtime, const Value &, const Value *, size_t count) {
            if (count != 0) throw JSError(runtime, "Jazz accountSecret takes no arguments");
            auto lock = lease->lockIfActive();
            if (!lock.owns_lock()) throw JSError(runtime, "Jazz native runtime is closed");
            jazz_native_relay_bytes output{nullptr, 0};
            const auto status = jazz_native_relay_account_secret(&output);
            if (status != JAZZ_NATIVE_RELAY_OK) throwStatus(runtime, status, "accountSecret");
            return foregroundResponse(runtime, &output);
          });
    }
    if (property == "mintLocalFirstToken") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "mintLocalFirstToken"), 4,
          [lease = lease_](Runtime &runtime, const Value &, const Value *args, size_t count) {
            if (count != 4 || !args[1].isString() || !args[2].isNumber() || !args[3].isNumber()) {
              throw JSError(runtime, "Jazz local-first mint requires seed, audience, TTL, and timestamp");
            }
            auto lock = lease->lockIfActive();
            if (!lock.owns_lock()) throw JSError(runtime, "Jazz native runtime is closed");
            const auto seed = copyForegroundCommand(runtime, args[0]);
            const auto audience = args[1].asString(runtime).utf8(runtime);
            const auto ttl = args[2].asNumber();
            const auto now = args[3].asNumber();
            constexpr double maxSafeInteger = 9007199254740991.0;
            if (seed.size() != 32 || !std::isfinite(ttl) || ttl <= 0 || ttl > maxSafeInteger ||
                std::floor(ttl) != ttl || !std::isfinite(now) || now < 0 || now > maxSafeInteger ||
                std::floor(now) != now) {
              throw JSError(runtime, "Jazz local-first mint arguments are invalid");
            }
            jazz_native_relay_bytes output{nullptr, 0};
            const auto status = jazz_native_relay_mint_local_first_token(
                seed.data(), seed.size(), reinterpret_cast<const uint8_t *>(audience.data()),
                audience.size(), static_cast<uint64_t>(ttl), static_cast<uint64_t>(now), &output);
            if (status != JAZZ_NATIVE_RELAY_OK) throwStatus(runtime, status, "mintLocalFirstToken");
            NativeResponseOwner ownership(&output);
            const std::string token(reinterpret_cast<const char *>(output.data), output.len);
            return Value(facebook::jsi::String::createFromUtf8(runtime, token));
          });
    }
    if (property == "openAttached") {
      return Function::createFromHostFunction(
          runtime, PropNameID::forAscii(runtime, "openAttached"), 1,
          [lease = lease_](Runtime &runtime, const Value &, const Value *args, size_t count) {
            if (count != 1) {
              throw JSError(runtime,
                           "Jazz native foreground runtime requires a 32-byte admitted capability");
            }
            auto lease_lock = lease->lockIfActive();
            if (!lease_lock.owns_lock()) {
              throw JSError(runtime, "Jazz native foreground runtime is unavailable after teardown");
            }
            const auto capability = copyAdmittedCapability(runtime, args[0]);
            uint64_t handle = 0;
            const auto status = jazz_native_relay_host_lease_open_attached_foreground(
                lease->nativeLease(), capability.data(), capability.size(), &handle);
            if (status != JAZZ_NATIVE_RELAY_OK) {
              throwStatus(runtime, status, "openAttached");
            }
            // ForegroundHandle registers its wake context with the same lease.
            // Do not construct it while holding this non-recursive lifecycle
            // mutex: registration must serialize with invalidation, but an
            // open that has not installed a Rust scheduler is safe to finish
            // after invalidation (its handle remains unusable).
            lease_lock.unlock();
            return Object::createFromHostObject(
                runtime, std::make_shared<ForegroundHandle>(lease, handle));
          });
    }
    return Value::undefined();
  }

  std::vector<PropNameID> getPropertyNames(Runtime &runtime) override {
    std::vector<PropNameID> names;
    names.reserve(8);
    names.emplace_back(PropNameID::forAscii(runtime, "beginAccountSession"));
    names.emplace_back(PropNameID::forAscii(runtime, "attachAccountSchema"));
    names.emplace_back(PropNameID::forAscii(runtime, "releaseAccountSession"));
    names.emplace_back(PropNameID::forAscii(runtime, "refreshAccountSession"));
    names.emplace_back(PropNameID::forAscii(runtime, "withAccountStoreLock"));
    names.emplace_back(PropNameID::forAscii(runtime, "accountSecret"));
    names.emplace_back(PropNameID::forAscii(runtime, "mintLocalFirstToken"));
    names.emplace_back(PropNameID::forAscii(runtime, "abiVersion"));
    names.emplace_back(PropNameID::forAscii(runtime, "openAttached"));
    return names;
  }

 private:
  std::shared_ptr<ForegroundRuntimeLease> lease_;
};

}  // namespace

void installForegroundRuntime(
    Runtime &runtime,
    const std::shared_ptr<ForegroundRuntimeLease> &lease) {
  if (!lease || !lease->active() || lease->nativeLease() == nullptr || !lease->callInvoker()) {
    throw JSError(runtime, "Jazz native relay host is unavailable");
  }
  runtime.global().setProperty(runtime, kWakeCallbacksGlobal, Object(runtime));
  runtime.global().setProperty(
      runtime, kFactoryGlobal,
      Object::createFromHostObject(runtime, std::make_shared<ForegroundFactory>(lease)));
}

}  // namespace jazz::rn
