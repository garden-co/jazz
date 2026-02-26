# This CMakeLists replaces RN's default-app-setup/CMakeLists.txt.
# It builds libappmodules.so (required by RN) AND libJazzNitro.so (Nitro Rust bridge).
cmake_minimum_required(VERSION 3.13)

# ── 1. Build libappmodules.so (React Native's required shared lib) ──────────
# project() name must be "appmodules" — RN's ReactNative-application.cmake
# creates the shared library using ${CMAKE_PROJECT_NAME}.
project(appmodules)

# ReactNative-application.cmake expects REACT_ANDROID_DIR and PROJECT_BUILD_DIR
# to be passed via cmake arguments (the RN gradle plugin does this automatically).
include(${REACT_ANDROID_DIR}/cmake-utils/ReactNative-application.cmake)

# ── 2. Build libJazzNitro.so (Nitro C++/Rust bridge) ───────────────────────
set(CMAKE_CXX_STANDARD 20)

# Create the JazzNitro shared library with the app's JNI adapter
add_library(JazzNitro SHARED
  "${CMAKE_SOURCE_DIR}/app/src/main/cpp/cpp-adapter.cpp"
)

# Nitrogen-generated autolinking adds: source files, include dirs, compile defs,
# find_package, link_libraries, Rust cargo build, and static lib linking.
include(${CMAKE_SOURCE_DIR}/../../../crates/jazz-nitro/nitrogen/generated/android/JazzNitro+autolinking.cmake)
