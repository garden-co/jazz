#!/bin/bash
# Rust build script
set -e

# Add cargo to PATH
export PATH="$HOME/.cargo/bin:$PATH"

# If CARGO is not set, use the default cargo
if [ -z "$CARGO" ]; then
  CARGO="cargo"
fi

echo "--- Rust Build Script ---"
echo "Platform: '$PLATFORM_NAME'"
echo "Architectures: '$ARCHS'"
echo "Configuration: '$CONFIGURATION'"
echo "SDK Name: '$SDK_NAME'"

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

echo "Detected Platform: $PLATFORM"
echo "Detected Platform Name: $PLATFORM_NAME"
echo "Detected Architectures: $ARCHS"

# Create necessary directories
mkdir -p includes/rust
mkdir -p src/cpp
mkdir -p src/generated
mkdir -p ios

# Flatten nitro headers
./build/flatten-nitro-headers.sh

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
export RUSTFLAGS="-C link-arg=-fPIC"

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

# Install cxxbridge if needed
$CARGO install cxxbridge-cmd --version 1.0.160 --locked

# Generate C++ headers
echo "Generating C++ headers..."
cxxbridge --header -o includes/rust/cxx.h
cxxbridge src/rust/lib.rs --header -o includes/rust/lib.rs.h
cxxbridge src/rust/lib.rs -o src/generated/lib.rs.cc

# Copy headers to src/generated as well
cp includes/rust/cxx.h src/generated/
cp includes/rust/lib.rs.h src/generated/

echo "Building Rust library for target: $RUST_TARGET"
$CARGO build --target "$RUST_TARGET" --release

# Copy the library to appropriate directories based on platform
echo "Copying library to output directories"

if [ "$PLATFORM" = "android" ]; then
  # Android: copy .so files to android/src/main/jniLibs
  mkdir -p "android/src/main/jniLibs/$ARCHS"
  cp "target/$RUST_TARGET/release/libjazz_crypto.so" "android/src/main/jniLibs/$ARCHS/"
else
  # iOS: copy .a files to ios directory for vendored_libraries
  mkdir -p ios
  cp "target/$RUST_TARGET/release/libjazz_crypto.a" "ios/"
fi

# Copy cxx bridge generated headers to includes/rust for Xcode build
echo "Copying cxx bridge generated headers..."
CXX_BRIDGE_HEADER=$(find "target/$RUST_TARGET/release/build" -name "lib.rs.h" -type f | head -1)
if [ -n "$CXX_BRIDGE_HEADER" ]; then
    mkdir -p "includes/rust"
    cp "$CXX_BRIDGE_HEADER" "includes/rust/"
    echo "Cxx bridge headers copied successfully."
else
    echo "Warning: Cxx bridge headers not found"
fi

echo "--- Build completed successfully ---"
