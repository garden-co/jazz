# CMakeLists.txt for JazzCrypto

cmake_minimum_required(VERSION 3.10)

project(JazzCrypto LANGUAGES CXX)

set(PACKAGE_NAME JazzCrypto)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Disable automatic generation of compile_commands.json for clangd
# We'll use a custom one for IDE integration
set(CMAKE_EXPORT_COMPILE_COMMANDS OFF)

# Add a library from the source files
add_library(
  ${PACKAGE_NAME} SHARED
  "../src/cpp/HybridJazzCrypto.cpp"
)

# add Nitrogen specs
include(${CMAKE_SOURCE_DIR}/../nitrogen/generated/android/JazzCrypto+autolinking.cmake)

# Specify include directories
include_directories(
  "../src/cpp"
)

# Third party libraries (Prefabs)
find_library(LOG_LIB log)

# Link all libraries together
target_link_libraries(
  ${PACKAGE_NAME}
  ${LOG_LIB}                               # <-- Logcat logger
  android                                  # <-- Android core
)

if(ReactAndroid_VERSION_MINOR GREATER_EQUAL 76)
  target_link_libraries(
    ${PACKAGE_NAME}
    ReactAndroid::reactnative              # <-- RN: Native Modules umbrella prefab
  )
else()
  target_link_libraries(
    ${PACKAGE_NAME}
    ReactAndroid::react_nativemodule_core  # <-- RN: TurboModules Core
    ReactAndroid::turbomodulejsijni        # <-- RN: TurboModules utils (e.g. CallInvokerHolder)
  )
endif()
