cmake_minimum_required(VERSION 3.22.1)
project(jazzrelay)

add_library(jazzrelay SHARED cpp-relay.cpp)
target_include_directories(jazzrelay PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../native/include")
find_library(log-lib log)
add_library(jazz_native_relay STATIC IMPORTED)
set_target_properties(jazz_native_relay PROPERTIES IMPORTED_LOCATION
  "${CMAKE_CURRENT_SOURCE_DIR}/src/main/jniLibs/${ANDROID_ABI}/libjazz_native_relay.a")
target_link_libraries(jazzrelay jazz_native_relay ${log-lib})
