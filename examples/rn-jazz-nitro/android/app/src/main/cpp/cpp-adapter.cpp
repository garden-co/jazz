#include <jni.h>
#include "JazzNitroOnLoad.hpp"

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
  return margelo::nitro::jazz::initialize(vm);
}
