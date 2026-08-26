#include <jni.h>

#include "jazz_native_relay.h"

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
  if (status != JAZZ_NATIVE_RELAY_OK) {
    env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), "Jazz native relay command failed");
    return nullptr;
  }
  auto result = env->NewByteArray(static_cast<jsize>(output.len));
  if (result != nullptr) {
    env->SetByteArrayRegion(result, 0, static_cast<jsize>(output.len),
                            reinterpret_cast<const jbyte *>(output.data));
  }
  jazz_native_relay_bytes_free(&output);
  return result;
}
