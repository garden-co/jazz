cmake_minimum_required(VERSION 3.22.1)
project(jazzrelay)

find_package(ReactAndroid REQUIRED CONFIG)
find_package(fbjni REQUIRED CONFIG)

add_library(jazzrelay SHARED cpp-relay.cpp ../native/foreground-runtime.cpp)
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
