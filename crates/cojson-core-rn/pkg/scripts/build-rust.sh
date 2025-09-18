#!/bin/bash
# Script to build Rust static library for React Native

set -e

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
CRATE_DIR="$SCRIPT_DIR/../.."
BUILD_DIR="$SCRIPT_DIR/../build"
CARGO="cargo"

# Create output directory for CI artifacts
if [ -n "$CI" ] || [ -n "$GITHUB_ACTIONS" ]; then
    OUTPUT_DIR="$HOME/output"
    mkdir -p "$OUTPUT_DIR"
    BUILD_LOG="$OUTPUT_DIR/rust-build.log"
    exec > >(tee -a "$BUILD_LOG") 2>&1
fi

echo "--- Rust Build Script ---"
echo "Platform     : '$PLATFORM'"
echo "Platform Name: '$PLATFORM_NAME'"
echo "Architectures: '$ARCHS'"
echo "Configuration: '$CONFIGURATION'"
echo "SDK Name     : '$SDK_NAME'"
echo "Android ABI  : '$ANDROID_ABI'"
echo "Current Arch : '$CURRENT_ARCH'"

# Prevent Xcode's iOS SDKROOT from contaminating cargo host builds. Set SDKROOT to macOS SDK so host linkers can find libSystem.
ORIG_SDKROOT="$SDKROOT"
if [[ "$(uname)" == "Darwin" ]]; then
  MACOS_SDK_PATH=$(xcrun --sdk macosx --show-sdk-path 2>/dev/null || true)
  if [ -n "$MACOS_SDK_PATH" ]; then
    export SDKROOT="$MACOS_SDK_PATH"
    echo "Configured SDKROOT for host builds to macOS SDK: $SDKROOT"
  else
    # As a fallback, unset SDKROOT to avoid iOS SDK poisoning
    if [ -n "$SDKROOT" ]; then
      echo "Could not resolve macOS SDK; unsetting SDKROOT to avoid iOS sysroot: $SDKROOT"
      unset SDKROOT
    fi
  fi
  # Ensure host CC/CXX are clang/clang++
  export CC=clang
  export CXX=clang++

  # Explicitly instruct cargo to use clang and macOS sysroot for host target link steps
  if [ -n "$MACOS_SDK_PATH" ]; then
    export CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=clang
    export CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER=clang
    export CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS="$CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS -C link-arg=-isysroot -C link-arg=$MACOS_SDK_PATH"
    export CARGO_TARGET_X86_64_APPLE_DARWIN_RUSTFLAGS="$CARGO_TARGET_X86_64_APPLE_DARWIN_RUSTFLAGS -C link-arg=-isysroot -C link-arg=$MACOS_SDK_PATH"
    echo "Configured host linker to use clang with macOS SDK: $MACOS_SDK_PATH"
  fi
fi

# Detect platform (iOS or Android)
if [ -n "$ANDROID_ABI" ]; then
  # Android build
  PLATFORM="android"
  # Map Android ABI to architecture name expected by the script
  case "$ANDROID_ABI" in
    aarch64-linux-android)
      ARCHS="arm64-v8a"
      ;;
    armv7-linux-androideabi)
      ARCHS="armeabi-v7a"
      ;;
    i686-linux-android)
      ARCHS="x86"
      ;;
    x86_64-linux-android)
      ARCHS="x86_64"
      ;;
    *)
      # If it's already in the expected format, use as-is
      ARCHS="$ANDROID_ABI"
      ;;
  esac
else
  # iOS build - detect from Xcode environment or set defaults
  PLATFORM="ios"
  
  # Try to detect platform from Xcode environment variables
  if [ -n "$SDK_NAME" ]; then
    if [[ "$SDK_NAME" == *"simulator"* ]]; then
      PLATFORM_NAME="iphonesimulator"
    else
      PLATFORM_NAME="iphoneos"
    fi
  elif [ -z "$PLATFORM_NAME" ]; then
    # Default to simulator for CI builds, device for local builds
    if [ -n "$CI" ] || [ -n "$GITHUB_ACTIONS" ]; then
      PLATFORM_NAME="iphonesimulator"
    else
      PLATFORM_NAME="iphoneos"
    fi
  fi
  
  # Handle architecture detection for both CI and local builds
  if [ -n "$CURRENT_ARCH" ] && [ "$CURRENT_ARCH" != "undefined_arch" ]; then
    # Local Xcode build - use the specific architecture
    ARCHS="$CURRENT_ARCH"
  elif [ -n "$ARCHS" ]; then
    # CI build or multi-arch build - ARCHS already set, keep it
    echo "Using provided architectures: $ARCHS"
  else
    # Fallback based on platform
    if [ "$PLATFORM_NAME" = "iphonesimulator" ]; then
      # For simulator, build both architectures for universal support
      ARCHS="arm64 x86_64"
    else
      # For device, just arm64
      ARCHS="arm64"
    fi
  fi
fi

# Create necessary directories
mkdir -p $BUILD_DIR/includes/rust
mkdir -p $BUILD_DIR/android
mkdir -p $BUILD_DIR/ios

# Flatten nitro headers
$SCRIPT_DIR/flatten-nitro-headers.sh $BUILD_DIR

# Determine Rust targets based on platform
if [ "$PLATFORM" = "android" ]; then
  # Android target mapping
  case "$ARCHS" in
    arm64-v8a)
      RUST_TARGETS="aarch64-linux-android"
      ;;
    armeabi-v7a)
      RUST_TARGETS="armv7-linux-androideabi"
      ;;
    x86)
      RUST_TARGETS="i686-linux-android"
      ;;
    x86_64)
      RUST_TARGETS="x86_64-linux-android"
      ;;
    *)
      echo "Unsupported Android architecture: $ARCHS"
      exit 1
      ;;
  esac
else
  # iOS target mapping - handle multiple architectures
  RUST_TARGETS=""
  case "$PLATFORM_NAME" in
    iphonesimulator)
      # Parse multiple architectures for simulator
      for arch in $ARCHS; do
        case "$arch" in
          x86_64)
            RUST_TARGETS="$RUST_TARGETS x86_64-apple-ios"
            ;;
          arm64)
            RUST_TARGETS="$RUST_TARGETS aarch64-apple-ios-sim"
            ;;
          *)
            echo "Unsupported simulator architecture: $arch"
            exit 1
            ;;
        esac
      done
      ;;
    *)
      # Default to physical device
      RUST_TARGETS="aarch64-apple-ios"
      ;;
  esac
  # Clean up extra spaces
  RUST_TARGETS=$(echo $RUST_TARGETS | xargs)
fi

echo "Building for $PLATFORM targets: $RUST_TARGETS"

# Set platform-specific configuration
if [ "$PLATFORM" != "android" ]; then
  # Set iOS-specific environment variables
  export IPHONEOS_DEPLOYMENT_TARGET=16.0
fi
export PKG_CONFIG_ALLOW_CROSS=1

# Set up Android NDK environment if building for Android
if [ "$PLATFORM" = "android" ]; then
  # Generate cargo config which handles NDK toolchain setup
  "$SCRIPT_DIR/setup-cargo-config.sh"
  
  # Set up Android NDK environment variables for cc-rs
  if [ -n "$ANDROID_HOME" ]; then
    NDK_PATH=$(find "$ANDROID_HOME/ndk" -maxdepth 1 -type d -name "[0-9]*" | sort -V | tail -1)
  elif [ -n "$ANDROID_NDK_ROOT" ]; then
    NDK_PATH="$ANDROID_NDK_ROOT"
  fi
  
  if [ -n "$NDK_PATH" ] && [ -d "$NDK_PATH" ]; then
    # Detect host OS for NDK prebuilt directory
    HOST_OS=$(uname -s)
    case "$HOST_OS" in
        Darwin)
            # Prefer Apple Silicon prebuilt if present to avoid Rosetta
            if [ -d "$NDK_PATH/toolchains/llvm/prebuilt/darwin-aarch64" ]; then
                NDK_HOST="darwin-aarch64"
            else
                NDK_HOST="darwin-x86_64"
            fi
            ;;
        Linux) NDK_HOST="linux-x86_64" ;;
        *) echo "Warning: Unsupported host OS: $HOST_OS, defaulting to linux-x86_64" >&2; NDK_HOST="linux-x86_64" ;;
    esac
    
    ANDROID_API_LEVEL="21"
    NDK_TOOLCHAIN_DIR="$NDK_PATH/toolchains/llvm/prebuilt/$NDK_HOST/bin"
    
    # Set CC/CXX environment variables for cc-rs
    export CC_aarch64_linux_android="$NDK_TOOLCHAIN_DIR/aarch64-linux-android$ANDROID_API_LEVEL-clang"
    export CXX_aarch64_linux_android="$NDK_TOOLCHAIN_DIR/aarch64-linux-android$ANDROID_API_LEVEL-clang++"
    export AR_aarch64_linux_android="$NDK_TOOLCHAIN_DIR/llvm-ar"
    
    export CC_armv7_linux_androideabi="$NDK_TOOLCHAIN_DIR/armv7a-linux-androideabi$ANDROID_API_LEVEL-clang"
    export CXX_armv7_linux_androideabi="$NDK_TOOLCHAIN_DIR/armv7a-linux-androideabi$ANDROID_API_LEVEL-clang++"
    export AR_armv7_linux_androideabi="$NDK_TOOLCHAIN_DIR/llvm-ar"
    
    export CC_i686_linux_android="$NDK_TOOLCHAIN_DIR/i686-linux-android$ANDROID_API_LEVEL-clang"
    export CXX_i686_linux_android="$NDK_TOOLCHAIN_DIR/i686-linux-android$ANDROID_API_LEVEL-clang++"
    export AR_i686_linux_android="$NDK_TOOLCHAIN_DIR/llvm-ar"
    
    export CC_x86_64_linux_android="$NDK_TOOLCHAIN_DIR/x86_64-linux-android$ANDROID_API_LEVEL-clang"
    export CXX_x86_64_linux_android="$NDK_TOOLCHAIN_DIR/x86_64-linux-android$ANDROID_API_LEVEL-clang++"
    export AR_x86_64_linux_android="$NDK_TOOLCHAIN_DIR/llvm-ar"
    
    echo "Configured Android NDK environment variables for cc-rs"
    echo "  NDK Path: $NDK_PATH"
    echo "  NDK Host: $NDK_HOST"
    echo "  API Level: $ANDROID_API_LEVEL"
  fi
fi

# perform builds from crate root
pushd $CRATE_DIR

# Install cxxbridge-cmd if not available
if ! command -v cxxbridge &> /dev/null; then
  echo "cxxbridge not found, installing..."
  cargo install cxxbridge-cmd
fi

# Generate C++ headers to build/includes/rust (not target directory to avoid ENAMETOOLONG)
echo "Generating C++ headers..."
cxxbridge --header -o $BUILD_DIR/includes/rust/cxx.h
cxxbridge src/lib.rs --header -o $BUILD_DIR/includes/rust/lib.rs.h

# Build for each target
for RUST_TARGET in $RUST_TARGETS; do
  echo "Building Rust library for target: $RUST_TARGET"
  
  # Use per-target directory outside workspace to prevent React Native CMake hanging
  # This allows concurrent builds and avoids CMake scanning deep build artifact trees
  TARGET_DIR="/tmp/jazz-rust-target-$RUST_TARGET"
  
  # Build with cargo - toolchain is configured via .cargo/config.toml
  $CARGO build --target "$RUST_TARGET" --release --target-dir "$TARGET_DIR"
  
  # Copy the library to appropriate directories based on platform
  echo "Copying library for target: $RUST_TARGET"
  
  if [ "$PLATFORM" = "android" ]; then
    # Android: copy .a files to architecture-specific directories
    case "$RUST_TARGET" in
      aarch64-linux-android)
        ANDROID_ABI="arm64-v8a"
        ;;
      armv7-linux-androideabi)
        ANDROID_ABI="armeabi-v7a"
        ;;
      i686-linux-android)
        ANDROID_ABI="x86"
        ;;
      x86_64-linux-android)
        ANDROID_ABI="x86_64"
        ;;
      *)
        echo "Unknown Android target: $RUST_TARGET"
        exit 1
        ;;
    esac
    
    mkdir -p "$BUILD_DIR/android/$ANDROID_ABI"
    cp "$TARGET_DIR/$RUST_TARGET/release/libcojson_core_rn.a" "$BUILD_DIR/android/$ANDROID_ABI/libcojson_core_rn.a"
  else
    # iOS: copy .a files to ios directory for vendored_libraries
    # For iOS, we need to create a universal library if multiple targets
    mkdir -p "$BUILD_DIR/ios"
    cp "$TARGET_DIR/$RUST_TARGET/release/libcojson_core_rn.a" "$BUILD_DIR/ios/libcojson_core_rn_$RUST_TARGET.a"
  fi
done

# For iOS with multiple targets, create a universal library
if [ "$PLATFORM" != "android" ] && [ $(echo $RUST_TARGETS | wc -w) -gt 1 ]; then
  echo "Creating universal iOS library..."
  LIPO_INPUTS=""
  for RUST_TARGET in $RUST_TARGETS; do
    LIPO_INPUTS="$LIPO_INPUTS $BUILD_DIR/ios/libcojson_core_rn_$RUST_TARGET.a"
  done
  
  # Create universal library
  lipo -create $LIPO_INPUTS -output "$BUILD_DIR/ios/libcojson_core_rn.a"
  
  # Clean up individual architecture libraries
  for RUST_TARGET in $RUST_TARGETS; do
    rm -f "$BUILD_DIR/ios/libcojson_core_rn_$RUST_TARGET.a"
  done
elif [ "$PLATFORM" != "android" ] && [ $(echo $RUST_TARGETS | wc -w) -eq 1 ]; then
  # Single target, just rename the file
  RUST_TARGET=$(echo $RUST_TARGETS | xargs)
  mv "$BUILD_DIR/ios/libcojson_core_rn_$RUST_TARGET.a" "$BUILD_DIR/ios/libcojson_core_rn.a"
fi

popd

echo "--- Build completed successfully ---"
echo "Artifacts copied to $BUILD_DIR, target directories preserved for incremental builds"

# Restore original SDKROOT if it was set
if [ -n "$ORIG_SDKROOT" ]; then
  export SDKROOT="$ORIG_SDKROOT"
fi
