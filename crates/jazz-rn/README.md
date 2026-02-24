# jazz-rn

A React Native native module that provides high-performance cryptographic operations for the Jazz framework, built with Rust and UniFFI. This package exposes the same cryptographic primitives as `jazz-napi` and `jazz-wasm` but specifically designed for React Native applications running on iOS and Android.

## What is jazz-rn?

`jazz-rn` is a React Native Turbo Module that bridges Rust-based cryptographic code to JavaScript/TypeScript. It uses [uniffi-bindgen-react-native](https://jhugman.github.io/uniffi-bindgen-react-native/) that uses [UniFFI](https://mozilla.github.io/uniffi-rs/) (Unified Foreign Function Interface) to automatically generate type-safe bindings between Rust and React Native, enabling you to use high-performance cryptographic operations in your React Native applications.

### Architecture

The package consists of:

- **Rust Core** (`rust/`): The core cryptographic implementation, shared with `jazz` and `jazz-napi`
- **UniFFI Bindings**: Automatically generated bindings that bridge Rust to React Native
- **Native Modules**:
  - **iOS**: XCFramework containing static libraries for arm64 (device) and arm64-simulator
  - **Android**: CMake-based native library compiled for multiple architectures
- **TypeScript Wrapper**: Type-safe JavaScript/TypeScript API that wraps the native bindings

## Installation

### In the Jazz Monorepo

If you're working within the Jazz monorepo, the package is already available as a workspace dependency:

```bash
# From the monorepo root
pnpm install
pnpm build:rn
```

### As a Standalone Package

```bash
pnpm install jazz-rn
```

The package includes pre-built native binaries for:

- **iOS**: arm64 (device) and arm64-simulator
- **Android**: Multiple architectures (arm64, arm, x86_64, etc.)

## Building from Source

### Prerequisites

Before building `jazz-rn`, ensure you followed this [guide](https://jhugman.github.io/uniffi-bindgen-react-native/guides/rn/pre-installation.html).
