cmake_minimum_required(VERSION 3.10)
project(JazzCrypto LANGUAGES CXX)

set(PACKAGE_NAME JazzCrypto)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 20)

# Add a library from the source files
add_library(
  ${PACKAGE_NAME} SHARED
  "src/main/cpp/cpp-adapter.cpp"
  "../src/cpp/HybridJazzCrypto.cpp"
)

# add Nitrogen specs
include(${CMAKE_SOURCE_DIR}/../nitrogen/generated/android/JazzCrypto+autolinking.cmake)

# Specify include directories
include_directories(
  "src/main/cpp"
  "../src/cpp"
  "../includes"
  "../includes/rust"
  "../nitrogen/generated/shared/c++"
  "../src/generated"
)

# Third party libraries (Prefabs)
find_library(LOG_LIB log)

# Link all libraries together
target_link_libraries(
  ${PACKAGE_NAME}
  ${LOG_LIB}                               # <-- Logcat logger
  android                                  # <-- Android core
)

# # Map Android ABI to Rust target for architecture-specific builds
# if(ANDROID_ABI STREQUAL "arm64-v8a")
#     set(RUST_TARGET "aarch64-linux-android")
# elseif(ANDROID_ABI STREQUAL "armeabi-v7a")
#     set(RUST_TARGET "armv7-linux-androideabi")
# elseif(ANDROID_ABI STREQUAL "x86")
#     set(RUST_TARGET "i686-linux-android")
# elseif(ANDROID_ABI STREQUAL "x86_64")
#     set(RUST_TARGET "x86_64-linux-android")
# else()
#     message(FATAL_ERROR "Unsupported Android ABI: ${ANDROID_ABI}")
# endif()

# # Find Rust static libraries for the current architecture
# set(RUST_MAIN_LIB "${CMAKE_CURRENT_SOURCE_DIR}/../target/${RUST_TARGET}/release/libjazz_crypto.a")
# if(NOT EXISTS ${RUST_MAIN_LIB})
#   message(FATAL_ERROR "Could not find main libjazz_crypto.a at ${RUST_MAIN_LIB} for ${RUST_TARGET}. Make sure the Rust build has completed.")
# endif()

# file(GLOB JAZZ_CRYPTO_LIB "${CMAKE_CURRENT_SOURCE_DIR}/../target/${RUST_TARGET}/release/build/jazz-crypto-*/out/libjazz_crypto.a")
# if(NOT JAZZ_CRYPTO_LIB)
#   message(FATAL_ERROR "Could not find libjazz_crypto.a from cxxbridge for ${RUST_TARGET}. Make sure the Rust build has completed.")
# endif()
# list(GET JAZZ_CRYPTO_LIB 0 JAZZ_CRYPTO_LIB_PATH)

# file(GLOB CXXBRIDGE_LIB "${CMAKE_CURRENT_SOURCE_DIR}/../target/${RUST_TARGET}/release/build/cxx-*/out/libcxxbridge1.a")
# if(NOT CXXBRIDGE_LIB)
#   message(FATAL_ERROR "Could not find libcxxbridge1.a for ${RUST_TARGET}. Make sure the Rust build has completed.")
# endif()
# list(GET CXXBRIDGE_LIB 0 CXXBRIDGE_LIB_PATH)

# # Link NitroModules shared library (required for symbol resolution)
# get_filename_component(PROJECT_ROOT "${CMAKE_SOURCE_DIR}/../../.." ABSOLUTE)
# set(NITRO_MODULES_LIB "${PROJECT_ROOT}/node_modules/react-native-nitro-modules/android/build/intermediates/cmake/debug/obj/${ANDROID_ABI}/libNitroModules.so")

# Link all required libraries with proper React Native version handling
if(ReactAndroid_VERSION_MINOR GREATER_EQUAL 76)
    target_link_libraries(
        ${PACKAGE_NAME}
        # ${NITRO_MODULES_LIB}                   # <-- NitroModules shared library
        # ${RUST_MAIN_LIB}                       # <-- Main Rust library with runtime
        # ${JAZZ_CRYPTO_LIB_PATH}                # <-- Our Rust+C++ bridge library
        # ${CXXBRIDGE_LIB_PATH}                  # <-- cxx bridge C++ runtime
        ReactAndroid::reactnative              # <-- RN: Native Modules umbrella prefab
    )
else()
    target_link_libraries(
        ${PACKAGE_NAME}
        # ${NITRO_MODULES_LIB}                   # <-- NitroModules shared library
        # ${RUST_MAIN_LIB}                       # <-- Main Rust library with runtime
        # ${JAZZ_CRYPTO_LIB_PATH}                # <-- Our Rust+C++ bridge library
        # ${CXXBRIDGE_LIB_PATH}                  # <-- cxx bridge C++ runtime
        ReactAndroid::react_nativemodule_core  # <-- RN: TurboModules Core
        ReactAndroid::turbomodulejsijni        # <-- RN: TurboModules utils (e.g. CallInvokerHolder)
    )
endif()
