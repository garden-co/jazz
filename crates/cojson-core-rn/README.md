# `cojson-core-rn` Maintainer Guide

This document provides a comprehensive guide for developers and future maintainers of the `cojson-core-rn` crate. Its purpose is to serve as a high-performance bridge between our React Native (JS/TS) application and our core crypto logic written in Rust.

## 1. Architecture Overview

The `cojson-core-rn` crate facilitates communication between two very different environments: the JavaScript world of React Native and the native world of Rust. It achieves this by creating a three-layer bridge:

```
[ React Native (JS/TS) ] <--> [ Nitro Module (C++) ] <--> [ Rust FFI Layer ]
```

1.  **React Native (JS/TS)**: The application-level code that consumes the crypto functionalities as if they were regular TypeScript methods.
2.  **Nitro Module (C++)**: The intermediary layer that acts as glue. It's a C++ module generated and managed by the [Nitro Modules](https://nitro.margelo.com//) framework. It exposes a native interface that can be called from JavaScript and, in turn, calls the Rust functions.
3.  **Rust FFI Layer**: This is where the performant core logic resides. Currently, this includes the `SessionLog` implementation, but it is expected that more crypto and core business logic will be migrated here over time. The Rust code is compiled into a native library that the C++ layer links against.

## 2. The Rust FFI Layer

The heart of this crate is the Rust code in `src/lib.rs`. It uses the `cxx` crate to create a safe and efficient Foreign Function Interface (FFI) boundary.

### Error Handling: No Panics!

**Critical rule**: Rust code exposed via FFI **must not panic**. A panic across an FFI boundary results in undefined behavior and will crash the application without a clear stack trace.

-   **DO NOT** use `.unwrap()` or `.expect()` on `Result` or `Option` types.
-   **DO NOT** use the `?` operator in a function that does not return a `Result` or `Option`.

Instead, every fallible function exposed to C++ must return a `Result`-like struct. We use a standardized `TransactionResult` struct for this purpose, which contains fields for `success`, `result` (on success), and `error` (on failure). The C++ layer can then inspect this struct and propagate errors gracefully.

```rust
// src/lib.rs

#[cxx::bridge]
mod ffi {
    struct TransactionResult {
        success: bool,
        result: String,
        error: String,
    }

    extern "Rust" {
        fn my_fallible_function(input: &str) -> TransactionResult;
    }
}

fn my_fallible_function(input: &str) -> TransactionResult {
    match do_something(input) {
        Ok(value) => TransactionResult { success: true, result: value, error: "".into() },
        Err(e) => TransactionResult { success: false, result: "".into(), error: e.to_string() },
    }
}
```

## 3. The C++ Bridge & Nitro Modules

Nitro Modules automate the creation of the C++ bridge code that connects JavaScript to native code.

### The Nitro Spec File

The API contract between JavaScript and C++ is defined in `pkg/src/cojson-core-rn.nitro.ts`. This TypeScript file serves as the single source of truth for the native module's interface.

When you change the Rust API, you **must** update this spec file to reflect the new function signatures, argument types, and return types.

### Regenerating C++ Bindings

After updating the `.nitro.ts` spec file, you must regenerate the C++ binding code. This is done by running a single command from `crates/cojson-core-rn/pkg/`:

```bash
pnpm specs
```

This command reads the spec file and automatically generates the necessary C++ header and implementation files inside `pkg/src/generated/`.

## 4. Workflow: How to Modify the Rust API

Here is the end-to-end process for adding or modifying a function in Rust and exposing it to React Native.

1.  **Modify the Rust Code (`src/lib.rs`)**
    -   Add or update your function within the `#[cxx::bridge]` module.
    -   Ensure it follows the no-panic error handling pattern by returning a `TransactionResult` or similar struct.

2.  **Update the Nitro Spec (`pkg/src/cojson-core-rn.nitro.ts`)**
    -   Modify the TypeScript interface to match the new Rust function's signature.

3.  **Regenerate C++ Bindings**
    -   Run `pnpm specs` from `crates/cojson-core-rn/pkg/` to update the generated C++ files.

4.  **Update the C++ Implementation (`pkg/src/cpp/HybridCoJSONCoreRN.cpp`)**
    -   This is the only manual C++ step. Open this file and implement the new method.
    -   You will need to call the corresponding Rust FFI function (e.g., `my_fallible_function(...)`).
    -   **Type Conversion**: Be mindful of converting types between C++ and Rust (e.g., `std::string` to `rust::String`, `ArrayBuffer` to `rust::Vec<u8>`). The `cxx` bridge provides helpers for this.

## 4.1. Integration with RNCrypto.ts

The `cojson-core-rn` module is consumed by the higher-level `RNCrypto.ts` file in the `jazz-tools` package. This integration layer:

-   **Direct Function Usage**: Instead of using wrapper classes, `RNCrypto.ts` directly imports and calls the individual cojson-core-rn functions (createSessionLog, tryAddTransactions, etc.).
-   **Handle Management**: The `RNSessionLog` class holds a `SessionLogHandle` directly and passes it between function calls, eliminating problematic handle conversions that could cause panics.
-   **Error Propagation**: Each method processes the `TransactionResult` pattern and throws JavaScript errors when `success` is false.
-   **Type Conversions**: Maintains proper ArrayBuffer/Uint8Array conversions for binary data like the `decryptNextTransactionChangesJson` method.

This architecture eliminates code duplication and provides a cleaner, more maintainable interface while preserving all existing functionality.

## 5. The Build System (`pkg/scripts/build-rust.sh`)

The script at `pkg/scripts/build-rust.sh` is responsible for compiling the Rust code into static libraries (`.a` for iOS and Android) for all required architectures.

This build script was particularly tricky to configure due to the complexities of cross-compilation for iOS and Android, especially from within an Xcode build environment. It handles:

-   **Platform Detection**: Uses environment variables like `PLATFORM_NAME` (iOS) and `ANDROID_ABI` (Android) to determine the target platform.  These envs should be set by Xcode and Gradle respectively.
-   **Architecture Mapping**: Maps platform architectures to Rust target triples:
    -   iOS: `arm64` → `aarch64-apple-ios`, `aarch64-apple-ios-sim`
    -   Android: `arm64-v8a` → `aarch64-linux-android`, `armeabi-v7a` → `armv7-linux-androideabi`, etc.
-   **SDK Configuration**: Sets correct linker and SDK paths (`-isysroot`) for each platform to prevent linker errors like `ld: library 'System' not found`.
-   **Host vs Target Isolation**: Uses target-specific `CARGO_TARGET_*_RUSTFLAGS` to ensure host builds (build.rs, proc-macros) use macOS SDK while iOS targets use iOS SDK.
-   **NDK Integration**: For Android, configures NDK toolchain with proper `CC`, `CXX`, `AR`, and `LINKER` environment variables.

### Build Integration

-   **iOS**: Automatically invoked by Xcode build phases. The `pkg/package.json` build script runs `build-rust.sh` before TypeScript compilation.
-   **Android**: Integrated into Gradle via `buildRustLibraries` task that runs before native compilation.
-   **Monorepo**: The `pnpm build:crates` command now automatically runs the Rust build, eliminating manual execution after fresh checkouts.

This script is invoked automatically by the iOS (Xcode) and Android (Gradle) build processes. Manual execution is typically only needed for debugging the build process itself.

## 6. IDE Support & Developer Experience (DX)

To ensure a smooth development experience with features like autocomplete and inline error checking, we have a special configuration.

The `pkg/build/` directory is **not used for production builds**. Instead, it is a designated location for artifacts generated specifically for IDE consumption.

-   **Purpose**: It holds generated C++ headers and other files that VSCode's C++ and Rust extensions need to understand the project structure and provide IntelliSense.
-   **Generation**: These files are generated by running the `pkg/scripts/setup_clang_env.sh` script, which creates a `compile_commands.json` file. This file is used by the C++ extension to resolve all necessary headers. (MacOS only)
-   **`.vscode/settings.json`**: This file is configured to point to the `compile_commands.json` file, allowing the C++ extension to resolve all necessary headers:
```json
{
    "clangd.arguments": [
        "--compile-commands-dir=${workspaceFolder}/crates/cojson-core-rn/pkg"
    ]
}
```

If you ever find that IntelliSense is broken, a good first step is to run the build script to regenerate these support files:

```bash
# From within crates/cojson-core-rn/pkg/
./scripts/setup_clang_env.sh
```

## 7. React Native Autolinking Configuration

The module uses React Native's autolinking system to automatically register with React Native projects. Key configuration files:

-   **`pkg/package.json`**: Must include `"react-native": "dist/index.js"` entry for autolinking discovery.
-   **`pkg/react-native.config.js`**: Defines platform-specific configuration for iOS and Android.
-   **`pkg/nitro.json`**: Nitro-specific configuration that defines the module's native interface.

### Android CMakeLists.txt

The Android build requires careful linking of multiple libraries:
-   **Rust static library**: Architecture-specific `.a` files (e.g., `libjazz_crypto.a`)
-   **cxxbridge runtime**: `libcxxbridge1.a` for C++/Rust FFI support
-   **NitroModules**: `libNitroModules.so` for Nitro framework integration

## 8. Troubleshooting Common Issues

### "ld: library 'System' not found" (iOS)
This occurs when Rust build scripts use incorrect SDK paths. The build script now uses target-specific `RUSTFLAGS` to isolate iOS and macOS SDK usage.

### Android Autolinking Failures
In monorepo setups, React Native autolinking may fail with null dependency errors. Solutions:
-   Ensure correct `sourceDir` path in autolinking configuration
-   Verify `react-native.config.js` and package.json entries are correct

### Missing C++ Headers
If IntelliSense fails or builds can't find headers:
```bash
# Regenerate IDE support files
cd crates/cojson-core-rn/pkg
./scripts/setup_clang_env.sh
```

### Panic Prevention
Always use the `TransactionResult` pattern for error handling. Never use:
-   `.unwrap()` or `.expect()` in FFI functions
-   `?` operator without proper Result return types
-   Direct panic-inducing operations across FFI boundaries

## 9. Future Considerations

As more crypto logic migrates from JavaScript to Rust:

1.  **API Expansion**: Follow the established pattern of Rust FFI → Nitro spec → C++ implementation
2.  **Error Handling**: Maintain consistent `TransactionResult` patterns for all new functions
3.  **Performance**: Consider batch operations for frequently called functions to reduce FFI overhead
4.  **Testing**: Add comprehensive tests for new Rust functions before exposing them via FFI
5.  **Documentation**: Update this README and inline code documentation as the API evolves