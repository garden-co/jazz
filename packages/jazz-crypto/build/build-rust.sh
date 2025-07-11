#!/bin/bash
# This script builds the Rust library for the correct Apple target
set -e

# TODO: nix
# Source the user's Nix profile to ensure cargo is in the PATH
# if [ -f "$HOME/.nix-profile/etc/profile.d/nix.sh" ]; then
#   . "$HOME/.nix-profile/etc/profile.d/nix.sh"
# fi

# No nix:
# Add common rustup locations to the PATH to find it.
# This makes the script more robust across different dev environments.
if [ -d "$HOME/.cargo/bin" ]; then
    export PATH="$HOME/.cargo/bin:$PATH"
fi
if [ -d "/opt/homebrew/bin" ]; then
    export PATH="/opt/homebrew/bin:$PATH"
fi
if [ -d "/usr/local/bin" ]; then
    export PATH="/usr/local/bin:$PATH"
fi

# Now that the path is likely set, find cargo.
CARGO=$(rustup which cargo)
if [ -z "$CARGO" ]; then
  echo "Error: rustup / cargo not found. Please ensure rustup is installed and cargo is in your PATH."
  exit 1
fi

# Add the directory containing cargo to the PATH. This is important because
# cargo needs to be able to find rustc and other tools.
CARGO_DIR=$(dirname "$CARGO")
export PATH="$CARGO_DIR:$PATH"

echo "--- Rust Build Script ---"
echo "Platform: '$PLATFORM_NAME', Architectures: '$ARCHS'"
echo "CARGO: '$CARGO'"

# special stuff for nitro-modules :old-man-yells-at-cloud:
./build/flatten-nitro-headers.sh

# Determine the Rust target based on the arguments
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

echo "Selected Rust target: $RUST_TARGET"

# The directory where the final library will be placed
UNIVERSAL_OUT_DIR="target/universal/debug"
FINAL_LIB_PATH="$UNIVERSAL_OUT_DIR/libjazz_crypto.dylib"

# Clean previous build artifacts for this target to avoid stale libs
echo "Cleaning previous build for $RUST_TARGET"
rm -rf "target/$RUST_TARGET"
rm -f "$FINAL_LIB_PATH"

# The cxx crate's build script needs to be compiled first for the host system.
# We run it separately to ensure it doesn't get cross-compilation flags.
echo "Building cxx bridge..."
cargo build --package cxx --target-dir target-cxx

# Now, build the actual library for the target iOS platform.
# We set the necessary environment variables for the C/C++ compiler and linker
# only for this command, ensuring they don't leak into other build steps.
SDK_PATH=$(xcrun --sdk $PLATFORM_NAME --show-sdk-path)

echo "Building Rust library for $RUST_TARGET..."
CC=clang CXX=clang \
CFLAGS="-isysroot $SDK_PATH -O3 -fembed-bitcode" \
CXXFLAGS="-isysroot $SDK_PATH -O3 -fembed-bitcode" \
cargo build --target "$RUST_TARGET"

# Create the universal directory
mkdir -p "$UNIVERSAL_OUT_DIR"

# Copy the built library to the universal directory
echo "Copying dylib to $FINAL_LIB_PATH"
cp "target/$RUST_TARGET/debug/libjazz_crypto.dylib" "$FINAL_LIB_PATH"

echo "--- Rust Build Script Finished ---"
