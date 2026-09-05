#include <jni.h>

#include <ReactCommon/BindingsInstallerHolder.h>
#include <fbjni/fbjni.h>

#include <memory>
#include <map>
#include <mutex>

#include "../native/foreground-runtime.h"
#include "jazz_native_relay.h"

namespace {
jbyteArray copy_response(JNIEnv *env, jazz_native_relay_bytes *output,
                         jazz_native_relay_status status,
                         const char *failure_message) {
  if (status != JAZZ_NATIVE_RELAY_OK) {
    env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), failure_message);
    return nullptr;
  }
  auto result = env->NewByteArray(static_cast<jsize>(output->len));
  if (result != nullptr) {
    env->SetByteArrayRegion(result, 0, static_cast<jsize>(output->len),
                            reinterpret_cast<const jbyte *>(output->data));
  }
  jazz_native_relay_bytes_free(output);
  return result;
}

struct ForegroundRuntimeInstallation {
  ForegroundRuntimeInstallation(
      jazz_native_relay_host *host,
      jlong runtime_token,
      const std::shared_ptr<facebook::react::CallInvoker> &callInvoker)
      : lease(std::make_shared<jazz::rn::ForegroundRuntimeLease>(
            host, static_cast<uint64_t>(runtime_token), callInvoker)) {}

  std::mutex mutex;
  std::shared_ptr<jazz::rn::ForegroundRuntimeLease> lease;
};

std::mutex foreground_installations_mutex;
// Android's bridge gives every React Native JSI runtime a monotonically
// allocated token. Keep that token in the native ownership key: a process can
// host more than one RN runtime against one durable relay host, and destroying
// one runtime must not invalidate any sibling's JSI lease.
using ForegroundRuntimeKey = std::pair<jazz_native_relay_host *, jlong>;
std::map<ForegroundRuntimeKey, std::shared_ptr<ForegroundRuntimeInstallation>>
    foreground_installations;

std::shared_ptr<ForegroundRuntimeInstallation> foregroundInstallation(
    jazz_native_relay_host *host,
    jlong runtime_token,
    const std::shared_ptr<facebook::react::CallInvoker> &callInvoker = nullptr) {
  std::lock_guard<std::mutex> lock(foreground_installations_mutex);
  const ForegroundRuntimeKey key{host, runtime_token};
  const auto found = foreground_installations.find(key);
  if (found != foreground_installations.end()) {
    return found->second;
  }
  if (!callInvoker) return nullptr;
  auto installation = std::make_shared<ForegroundRuntimeInstallation>(
      host, runtime_token, callInvoker);
  foreground_installations.emplace(key, installation);
  return installation;
}
}  // namespace

extern "C" JNIEXPORT jlong JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeCreate(JNIEnv *, jclass) {
  return reinterpret_cast<jlong>(jazz_native_relay_host_new());
}

extern "C" JNIEXPORT void JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeDestroy(JNIEnv *, jclass, jlong host) {
  jazz_native_relay_host_free(reinterpret_cast<jazz_native_relay_host *>(host));
}

extern "C" JNIEXPORT jint JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeAbiVersion(JNIEnv *, jclass) {
  return jazz_native_relay_abi_version();
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeExecute(
    JNIEnv *env, jclass, jlong host, jbyteArray command) {
  const jsize length = env->GetArrayLength(command);
  jbyte *input = env->GetByteArrayElements(command, nullptr);
  jazz_native_relay_bytes output{};
  const auto status = jazz_native_relay_host_execute(
      reinterpret_cast<jazz_native_relay_host *>(host),
      reinterpret_cast<const uint8_t *>(input), static_cast<size_t>(length), &output);
  env->ReleaseByteArrayElements(command, input, JNI_ABORT);
  return copy_response(env, &output, status, "Jazz native relay command failed");
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeAdmitTrustedScopeJson(
    JNIEnv *env, jclass, jlong host, jbyteArray admission_json) {
  const jsize length = env->GetArrayLength(admission_json);
  jbyte *input = env->GetByteArrayElements(admission_json, nullptr);
  jazz_native_relay_bytes output{};
  const auto status = jazz_native_relay_host_admit_scope_json(
      reinterpret_cast<jazz_native_relay_host *>(host),
      reinterpret_cast<const uint8_t *>(input), static_cast<size_t>(length), &output);
  env->ReleaseByteArrayElements(admission_json, input, JNI_ABORT);
  return copy_response(env, &output, status,
                       "Jazz trusted relay admission was rejected");
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeBeginPrivateSessionJson(
    JNIEnv *env, jclass, jlong host, jbyteArray session_json) {
  const jsize length = env->GetArrayLength(session_json);
  jbyte *input = env->GetByteArrayElements(session_json, nullptr);
  jazz_native_relay_bytes output{};
  const auto status = jazz_native_relay_host_begin_private_session_json(
      reinterpret_cast<jazz_native_relay_host *>(host),
      reinterpret_cast<const uint8_t *>(input), static_cast<size_t>(length), &output);
  env->ReleaseByteArrayElements(session_json, input, JNI_ABORT);
  return copy_response(env, &output, status, "Jazz private relay session setup was rejected");
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeAttachCanonicalSchemaJson(
    JNIEnv *env, jclass, jlong host, jbyteArray capability, jbyteArray schema_json) {
  const jsize capability_length = env->GetArrayLength(capability);
  const jsize schema_length = env->GetArrayLength(schema_json);
  jbyte *capability_bytes = env->GetByteArrayElements(capability, nullptr);
  jbyte *schema_bytes = env->GetByteArrayElements(schema_json, nullptr);
  jazz_native_relay_bytes output{};
  const auto status = jazz_native_relay_host_attach_canonical_schema_json(
      reinterpret_cast<jazz_native_relay_host *>(host),
      reinterpret_cast<const uint8_t *>(capability_bytes), static_cast<size_t>(capability_length),
      reinterpret_cast<const uint8_t *>(schema_bytes), static_cast<size_t>(schema_length), &output);
  env->ReleaseByteArrayElements(capability, capability_bytes, JNI_ABORT);
  env->ReleaseByteArrayElements(schema_json, schema_bytes, JNI_ABORT);
  return copy_response(env, &output, status, "Jazz canonical relay schema attachment was rejected");
}

extern "C" JNIEXPORT void JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeRevokeTrustedScope(
    JNIEnv *env, jclass, jlong host, jbyteArray capability) {
  const jsize length = env->GetArrayLength(capability);
  jbyte *input = env->GetByteArrayElements(capability, nullptr);
  const auto status = jazz_native_relay_host_revoke_scope_capability(
      reinterpret_cast<jazz_native_relay_host *>(host),
      reinterpret_cast<const uint8_t *>(input), static_cast<size_t>(length));
  env->ReleaseByteArrayElements(capability, input, JNI_ABORT);
  if (status != JAZZ_NATIVE_RELAY_OK) {
    env->ThrowNew(env->FindClass("java/lang/IllegalStateException"),
                  "Jazz trusted relay revocation failed");
  }
}

extern "C" JNIEXPORT jobject JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeForegroundBindingsInstaller(
    JNIEnv *, jclass, jlong host, jlong runtime_token) {
  auto *relay_host = reinterpret_cast<jazz_native_relay_host *>(host);
  if (relay_host == nullptr) {
    return nullptr;
  }
  auto holder = facebook::react::BindingsInstallerHolder::newObjectCxxArgs(
      [relay_host, runtime_token](facebook::jsi::Runtime &runtime,
                     const std::shared_ptr<facebook::react::CallInvoker> &callInvoker) {
        auto installation = foregroundInstallation(relay_host, runtime_token, callInvoker);
        if (!installation) return;
        std::lock_guard<std::mutex> lock(installation->mutex);
        jazz::rn::installForegroundRuntime(runtime, installation->lease);
      });
  return holder.release();
}

extern "C" JNIEXPORT void JNICALL
Java_com_jazzrn_JazzRelayBridge_nativeInvalidateForegroundRuntime(
    JNIEnv *, jclass, jlong host, jlong runtime_token) {
  auto *relay_host = reinterpret_cast<jazz_native_relay_host *>(host);
  std::lock_guard<std::mutex> lock(foreground_installations_mutex);
  const auto found = foreground_installations.find({relay_host, runtime_token});
  if (found == foreground_installations.end()) {
    return;
  }
  std::lock_guard<std::mutex> installation_lock(found->second->mutex);
  found->second->lease->invalidate();
  foreground_installations.erase(found);
}
