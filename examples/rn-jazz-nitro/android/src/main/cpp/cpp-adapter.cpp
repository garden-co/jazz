///
/// cpp-adapter.cpp
/// JNI entry point for JazzNitro — calls the generated initialize() which
/// registers the JazzRuntime HybridObject with Nitro's registry.
///

#include <jni.h>
#include "JazzNitroOnLoad.hpp"

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
  return margelo::nitro::jazz::initialize(vm);
}
