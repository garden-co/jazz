cmake_minimum_required(VERSION 3.10)
project(CoJSONCoreRN LANGUAGES CXX)

set(PACKAGE_NAME CoJSONCoreRN)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 20)

# Add a library from the source files
add_library(
  ${PACKAGE_NAME} SHARED
  "src/main/cpp/cpp-adapter.cpp"
  "../src/cpp/HybridCoJSONCoreRN.cpp"
)

# add Nitrogen specs
include(${CMAKE_SOURCE_DIR}/../nitrogen/generated/android/CoJSONCoreRN+autolinking.cmake)

# Specify include directories
include_directories(
  "src/main/cpp"
  "../pkg/cpp"
  "../includes"
  "../includes/rust"
  "../nitrogen/generated/shared/c++"
  "../generated"
)

# Third party libraries (Prefabs)
find_library(LOG_LIB log)

# Link all libraries together
target_link_libraries(
  ${PACKAGE_NAME}
  ${LOG_LIB}                               # <-- Logcat logger
  android                                  # <-- Android core
)

# Link all required libraries with proper React Native version handling
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
