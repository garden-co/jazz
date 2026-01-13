#!/bin/bash
set -e

# Setup script for cojson-core-rn
# Based on: https://jhugman.github.io/uniffi-bindgen-react-native/guides/rn/pre-installation.html

echo "=== cojson-core-rn Setup Script ==="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_command() {
    if command -v "$1" &> /dev/null; then
        echo -e "${GREEN}✓${NC} $1 is installed"
        return 0
    else
        echo -e "${RED}✗${NC} $1 is not installed"
        return 1
    fi
}

# Detect OS
OS="$(uname -s)"
echo "Detected OS: $OS"
echo ""

# =============================================================================
# Check Prerequisites
# =============================================================================
echo "=== Checking Prerequisites ==="
echo ""

# Check Rust
if check_command rustc; then
    echo "  Version: $(rustc --version)"
else
    echo ""
    echo -e "${YELLOW}Install Rust from https://rustup.rs/${NC}"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

# Check Cargo
if ! check_command cargo; then
    echo "Cargo should be installed with Rust. Please reinstall Rust."
    exit 1
fi

# Check rustup
if ! check_command rustup; then
    echo "rustup should be installed with Rust. Please reinstall Rust."
    exit 1
fi

echo ""

# =============================================================================
# Check C++ Tooling
# =============================================================================
echo "=== Checking C++ Tooling ==="
echo ""

MISSING_TOOLS=()

check_command cmake || MISSING_TOOLS+=("cmake")
check_command ninja || MISSING_TOOLS+=("ninja")

# clang-format is optional
if check_command clang-format; then
    echo "  (clang-format is optional)"
fi

if [ ${#MISSING_TOOLS[@]} -ne 0 ]; then
    echo ""
    echo -e "${YELLOW}Missing tools: ${MISSING_TOOLS[*]}${NC}"
    if [ "$OS" = "Darwin" ]; then
        echo "Install with: brew install ${MISSING_TOOLS[*]}"
    else
        echo "Install with: apt-get install ${MISSING_TOOLS[*]}"
    fi
    exit 1
fi

echo ""

# =============================================================================
# Install Rust Targets
# =============================================================================
echo "=== Installing Rust Targets ==="
echo ""

# Android targets
echo "Adding Android targets..."
rustup target add aarch64-linux-android armv7-linux-androideabi i686-linux-android x86_64-linux-android

# iOS targets (macOS only)
if [ "$OS" = "Darwin" ]; then
    echo "Adding iOS targets..."
    rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
fi

echo -e "${GREEN}✓${NC} Rust targets installed"
echo ""

# =============================================================================
# Install cargo-ndk (Android)
# =============================================================================
echo "=== Installing cargo-ndk ==="
echo ""

if cargo install --list | grep -q "cargo-ndk"; then
    echo -e "${GREEN}✓${NC} cargo-ndk is already installed"
else
    echo "Installing cargo-ndk..."
    cargo install cargo-ndk
    echo -e "${GREEN}✓${NC} cargo-ndk installed"
fi

echo ""

# =============================================================================
# Check Android SDK (optional)
# =============================================================================
echo "=== Checking Android SDK ==="
echo ""

if [ -n "$ANDROID_HOME" ]; then
    echo -e "${GREEN}✓${NC} ANDROID_HOME is set: $ANDROID_HOME"
elif [ -n "$ANDROID_SDK_ROOT" ]; then
    echo -e "${GREEN}✓${NC} ANDROID_SDK_ROOT is set: $ANDROID_SDK_ROOT"
else
    echo -e "${YELLOW}!${NC} ANDROID_HOME/ANDROID_SDK_ROOT not set"
    echo "  Set this to your Android SDK location if you need Android builds"
    if [ "$OS" = "Darwin" ]; then
        echo "  Typical location: ~/Library/Android/sdk"
    else
        echo "  Typical location: ~/Android/Sdk"
    fi
fi

# Check NDK
if [ -n "$ANDROID_NDK_HOME" ] || [ -n "$ANDROID_NDK_LATEST_HOME" ]; then
    echo -e "${GREEN}✓${NC} Android NDK is configured"
else
    echo -e "${YELLOW}!${NC} ANDROID_NDK_HOME not set"
    echo "  Install NDK via Android Studio: SDK Manager > SDK Tools > NDK"
fi

echo ""

# =============================================================================
# Check Xcode (macOS only)
# =============================================================================
if [ "$OS" = "Darwin" ]; then
    echo "=== Checking Xcode ==="
    echo ""

    if xcode-select -p &> /dev/null; then
        echo -e "${GREEN}✓${NC} Xcode Command Line Tools installed"
        echo "  Path: $(xcode-select -p)"
    else
        echo -e "${RED}✗${NC} Xcode Command Line Tools not installed"
        echo "  Run: xcode-select --install"
        exit 1
    fi

    echo ""
fi

# =============================================================================
# Summary
# =============================================================================
echo "=== Setup Complete ==="
echo ""
echo -e "${GREEN}All required dependencies are installed!${NC}"
echo ""
echo "You can now build cojson-core-rn:"
echo "  pnpm build:rn        # Build for all platforms"
echo "  pnpm build:rn:ios    # Build for iOS only"
echo "  pnpm build:rn:android # Build for Android only (in crates/cojson-core-rn)"
echo ""
