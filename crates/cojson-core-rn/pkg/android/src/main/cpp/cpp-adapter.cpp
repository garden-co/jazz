#include <fbjni/fbjni.h>
#include <jni.h>

#include "CoJSONCoreRNOnLoad.hpp"

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
  return facebook::jni::initialize(vm, [=] { 
    margelo::nitro::cojson_core_rn::initialize(vm); 
  });
}
