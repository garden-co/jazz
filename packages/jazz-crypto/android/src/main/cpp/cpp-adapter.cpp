#include <fbjni/fbjni.h>
#include <jni.h>

#include "JazzCryptoOnLoad.hpp"

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
  return facebook::jni::initialize(vm, [=] { margelo::nitro::jazz_crypto::initialize(vm); });
}
