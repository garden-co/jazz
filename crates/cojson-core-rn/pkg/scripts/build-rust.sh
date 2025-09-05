#!/bin/bash
# Rust build script
set -e

# Add some usual cargo locations to PATH
export PATH="$HOME/.cargo/bin:$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$PATH"

# If CARGO is not set, use the default cargo
if [ -z "$CARGO" ]; then
  CARGO="cargo"
fi

echo "--- Rust Build Script ---"
echo "Platform     : '$PLATFORM'"
echo "Platform Name: '$PLATFORM_NAME'"
echo "Architectures: '$ARCHS'"
echo "Configuration: '$CONFIGURATION'"
echo "SDK Name     : '$SDK_NAME'"
echo "Android ABI  : '$ANDROID_ABI'"
echo "Current Arch : '$CURRENT_ARCH'"

# Detect platform (iOS or Android)
if [ -n "$ANDROID_ABI" ]; then
  # Android build
  PLATFORM="android"
  ARCHS="$ANDROID_ABI"
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
    PLATFORM_NAME="iphoneos"
  fi
  
  # Try to detect architecture from Xcode environment
  if [ -n "$CURRENT_ARCH" ] && [ "$CURRENT_ARCH" != "undefined_arch" ]; then
    ARCHS="$CURRENT_ARCH"
  elif [ -z "$ARCHS" ]; then
    ARCHS="arm64"
  fi
fi

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
BUILD_DIR="$SCRIPT_DIR/../build"
CRATE_DIR="$SCRIPT_DIR/../.."
CRATES_DIR="$CRATE_DIR/.."

# Create necessary directories
mkdir -p $BUILD_DIR/includes/rust
mkdir -p $BUILD_DIR/android
mkdir -p $BUILD_DIR/ios

# Flatten nitro headers
$SCRIPT_DIR/flatten-nitro-headers.sh $BUILD_DIR

# Determine Rust target based on platform
if [ "$PLATFORM" = "android" ]; then
  # Android target mapping
  case "$ARCHS" in
    arm64-v8a)
      RUST_TARGET="aarch64-linux-android"
      ;;
    armeabi-v7a)
      RUST_TARGET="armv7-linux-androideabi"
      ;;
    x86)
      RUST_TARGET="i686-linux-android"
      ;;
    x86_64)
      RUST_TARGET="x86_64-linux-android"
      ;;
    *)
      echo "Unsupported Android architecture: $ARCHS"
      exit 1
      ;;
  esac
else
  # iOS target mapping
  case "$PLATFORM_NAME" in
    iphonesimulator)
      case "$ARCHS" in
        *x86_64*)
          RUST_TARGET="x86_64-apple-ios"
          ;;
        *arm64*)
          RUST_TARGET="aarch64-apple-ios-sim"
          ;;
        *)
          echo "Unsupported simulator architecture: $ARCHS"
          exit 1
          ;;
      esac
      ;;
    *)
      # Default to physical device
      RUST_TARGET="aarch64-apple-ios"
      ;;
  esac
fi

echo "Building for $PLATFORM target: $RUST_TARGET"

# Set build flags
export CXXFLAGS="-std=c++20 -fPIC"

# Set platform-specific linker flags
if [ "$PLATFORM" != "android" ]; then
  # iOS: Set SDK path and use framework linking
  if [ "$PLATFORM_NAME" = "iphonesimulator" ]; then
    SDK_PATH="/Applications/Xcode.app/Contents/Developer/Platforms/iPhoneSimulator.platform/Developer/SDKs/iPhoneSimulator.sdk"
  else
    SDK_PATH="/Applications/Xcode.app/Contents/Developer/Platforms/iPhoneOS.platform/Developer/SDKs/iPhoneOS.sdk"
  fi
  export RUSTFLAGS="-C link-arg=-fPIC -C link-arg=-isysroot -C link-arg=$SDK_PATH"
else
  export RUSTFLAGS="-C link-arg=-fPIC"
fi

# Set up Android NDK environment if building for Android
if [ "$PLATFORM" = "android" ]; then
  # Try to find Android NDK
  if [ -n "$ANDROID_NDK_ROOT" ]; then
    NDK_PATH="$ANDROID_NDK_ROOT"
  elif [ -n "$ANDROID_NDK_HOME" ]; then
    NDK_PATH="$ANDROID_NDK_HOME"
  elif [ -d "$HOME/Library/Android/sdk/ndk" ]; then
    # macOS default Android Studio NDK location - use latest version
    NDK_PATH=$(find "$HOME/Library/Android/sdk/ndk" -maxdepth 1 -type d -name "[0-9]*" | sort -V | tail -1)
  else
    echo "Error: Android NDK not found. Please set ANDROID_NDK_ROOT or install Android NDK."
    exit 1
  fi
  
  echo "Using Android NDK at: $NDK_PATH"
  
  # Set up NDK toolchain environment
  case "$RUST_TARGET" in
    aarch64-linux-android)
      export CC="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/aarch64-linux-android21-clang"
      export CXX="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/aarch64-linux-android21-clang++"
      export AR="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-ar"
      export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/aarch64-linux-android21-clang"
      ;;
    armv7-linux-androideabi)
      export CC="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/armv7a-linux-androideabi21-clang"
      export CXX="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/armv7a-linux-androideabi21-clang++"
      export AR="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-ar"
      export CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_LINKER="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/armv7a-linux-androideabi21-clang"
      ;;
    i686-linux-android)
      export CC="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/i686-linux-android21-clang"
      export CXX="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/i686-linux-android21-clang++"
      export AR="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-ar"
      export CARGO_TARGET_I686_LINUX_ANDROID_LINKER="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/i686-linux-android21-clang"
      ;;
    x86_64-linux-android)
      export CC="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/x86_64-linux-android21-clang"
      export CXX="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/x86_64-linux-android21-clang++"
      export AR="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/llvm-ar"
      export CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER="$NDK_PATH/toolchains/llvm/prebuilt/darwin-x86_64/bin/x86_64-linux-android21-clang"
      ;;
  esac
fi

# perform builds from crate root
pushd $CRATE_DIR

# Generate C++ headers
echo "Generating C++ headers..."
cxxbridge --header -o $BUILD_DIR/includes/rust/cxx.h
cxxbridge src/lib.rs --header -o $BUILD_DIR/includes/rust/lib.rs.h

echo "Building Rust library for target: $RUST_TARGET"
$CARGO build --target "$RUST_TARGET" --release

# Copy the library to appropriate directories based on platform
echo "Copying library to output directories"

if [ "$PLATFORM" = "android" ]; then
  # Android: copy .so files to android/{arch}
  mkdir -p "$BUILD_DIR/android/$ARCHS"
  cp "$CRATES_DIR/target/$RUST_TARGET/release/libcojson_core_rn.so" "$BUILD_DIR/android/$ARCHS/"
else
  # iOS: copy .a files to ios directory for vendored_libraries
  cp "$CRATES_DIR/target/$RUST_TARGET/release/libcojson_core_rn.a" "$BUILD_DIR/ios/"
fi

# Copy cxx bridge generated headers to includes/rust for Xcode build
echo "Copying cxx bridge generated headers..."
CXX_BRIDGE_HEADER=$(find "$CRATES_DIR/target/$RUST_TARGET/release/build" -name "lib.rs.h" -type f | head -1)
if [ -n "$CXX_BRIDGE_HEADER" ]; then
    cp "$CXX_BRIDGE_HEADER" "$BUILD_DIR/includes/rust/"
    echo "Cxx bridge headers copied successfully."
else
    echo "Warning: Cxx bridge headers not found"
fi

popd

echo "--- Build completed successfully ---"
