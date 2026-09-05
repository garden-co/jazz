cmake_minimum_required(VERSION 3.22.1)
project(jazzrelay)

find_package(ReactAndroid REQUIRED CONFIG)
find_package(fbjni REQUIRED CONFIG)

add_library(jazzrelay SHARED cpp-relay.cpp ../native/foreground-runtime.cpp)
# React Native 0.81's public headers use C++20 concepts. Keep the requirement
# target-scoped so this library cannot depend on an app's incidental global
# CMake standard or silently compile in an older language mode.
target_compile_features(jazzrelay PRIVATE cxx_std_20)
set_target_properties(jazzrelay PROPERTIES CXX_EXTENSIONS OFF)
target_include_directories(jazzrelay PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/../native"
  "${CMAKE_CURRENT_SOURCE_DIR}/../native/include"
  "${REACT_NATIVE_DIR}/ReactAndroid/src/main/jni/react/turbomodule"
)
find_library(log-lib log)
add_library(jazz_native_relay STATIC IMPORTED)
set_target_properties(jazz_native_relay PROPERTIES IMPORTED_LOCATION
  "${CMAKE_CURRENT_SOURCE_DIR}/src/main/jniLibs/${ANDROID_ABI}/libjazz_native_relay.a")
target_link_libraries(jazzrelay
  jazz_native_relay
  ${log-lib}
  fbjni::fbjni
  ReactAndroid::jsi
  ReactAndroid::reactnative
)
