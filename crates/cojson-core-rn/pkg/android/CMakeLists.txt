cmake_minimum_required(VERSION 3.10)
project(CoJSONCoreRN)

set(PACKAGE_NAME CoJSONCoreRN)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 20)

# Architecture mapping for Rust targets
if(ANDROID_ABI STREQUAL "arm64-v8a")
    set(RUST_TARGET_ARCH "arm64-v8a")
elseif(ANDROID_ABI STREQUAL "armeabi-v7a")
    set(RUST_TARGET_ARCH "armeabi-v7a")
elseif(ANDROID_ABI STREQUAL "x86")
    set(RUST_TARGET_ARCH "x86")
elseif(ANDROID_ABI STREQUAL "x86_64")
    set(RUST_TARGET_ARCH "x86_64")
else()
    message(FATAL_ERROR "Unsupported Android ABI: ${ANDROID_ABI}")
endif()

# Add a library from the source files
add_library(${PACKAGE_NAME} SHARED
  "src/main/cpp/cpp-adapter.cpp"
  "../cpp/HybridCoJSONCoreRN.cpp"
)

# add Nitrogen specs
include(${CMAKE_SOURCE_DIR}/../nitrogen/generated/android/CoJSONCoreRN+autolinking.cmake)

# Specify include directories
include_directories(
  "src/main/cpp"
  "../cpp"
  "../build/includes"
  "../build/includes/rust"
  "../nitrogen/generated/shared/c++"
)

# Third party libraries (Prefabs)
find_library(LOG_LIB log)

# Map Android ABI to Rust target for both cxxbridge and static library paths
if(ANDROID_ABI STREQUAL "arm64-v8a")
    set(RUST_TARGET "aarch64-linux-android")
elseif(ANDROID_ABI STREQUAL "armeabi-v7a")
    set(RUST_TARGET "armv7-linux-androideabi")
elseif(ANDROID_ABI STREQUAL "x86")
    set(RUST_TARGET "i686-linux-android")
elseif(ANDROID_ABI STREQUAL "x86_64")
    set(RUST_TARGET "x86_64-linux-android")
endif()

# Import Rust static library from build directory (copied by build-rust.sh)
set(BUILD_DIR "${CMAKE_SOURCE_DIR}/../build")
set(RUST_STATIC_LIB_PATH "${BUILD_DIR}/android/${RUST_TARGET_ARCH}/libcojson_core_rn.a")
add_library(cojson_core_rn_rust STATIC IMPORTED)
set_target_properties(cojson_core_rn_rust PROPERTIES IMPORTED_LOCATION ${RUST_STATIC_LIB_PATH})

# Find and link cxxbridge runtime library
file(GLOB_RECURSE CXXBRIDGE_LIB "${CMAKE_SOURCE_DIR}/../../target/${RUST_TARGET}/release/build/**/libcxxbridge1.a")
message(STATUS "CXXBRIDGE_LIB: ${CXXBRIDGE_LIB}")
if(CXXBRIDGE_LIB)
    add_library(cxxbridge1 STATIC IMPORTED)
    set_target_properties(cxxbridge1 PROPERTIES IMPORTED_LOCATION ${CXXBRIDGE_LIB})
    message(STATUS "Found cxxbridge library: ${CXXBRIDGE_LIB}")
else()
    message(STATUS "cxxbridge library not found, will be available after Gradle buildRustStaticLibs")
    add_library(cxxbridge1 STATIC IMPORTED)
    set_target_properties(cxxbridge1 PROPERTIES IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/../../target/${RUST_TARGET}/release/build/dummy/libcxxbridge1.a")
endif()

# Link NitroModules shared library
set(PROJECT_ROOT "${CMAKE_SOURCE_DIR}/../../../..")
set(NITRO_LIB_PATH "${PROJECT_ROOT}/node_modules/react-native-nitro-modules/android/build/intermediates/cmake/debug/obj/${ANDROID_ABI}/libNitroModules.so")
add_library(NitroModules SHARED IMPORTED)
set_target_properties(NitroModules PROPERTIES IMPORTED_LOCATION ${NITRO_LIB_PATH})

# Link all libraries together
target_link_libraries(
  ${PACKAGE_NAME}
  ${LOG_LIB}                               # <-- Logcat logger
  android                                  # <-- Android core
  cojson_core_rn_rust                      # <-- Rust library
  NitroModules                             # <-- NitroModules shared library
)

# Link cxxbridge if it exists
if(CXXBRIDGE_LIB)
  target_link_libraries(${PACKAGE_NAME} cxxbridge1)
endif()

# Uncomment if you want to try with earlier RN versions... gl;hf!
# # Link all required libraries with proper React Native version handling
# if(ReactAndroid_VERSION_MINOR GREATER_EQUAL 76)
#     target_link_libraries(
#         ${PACKAGE_NAME}
#         ReactAndroid::reactnative              # <-- RN: Native Modules umbrella prefab
#     )
# else()
#     target_link_libraries(
#         ${PACKAGE_NAME}
#         ReactAndroid::react_nativemodule_core  # <-- RN: TurboModules Core
#         ReactAndroid::turbomodulejsijni        # <-- RN: TurboModules utils (e.g. CallInvokerHolder)
#     )
# endif()
