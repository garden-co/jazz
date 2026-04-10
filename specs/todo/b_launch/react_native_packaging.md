# React Native Packaging — TODO

Package jazz2 for React Native with native storage.

## Overview

React Native can't use OPFS or Web Workers. Need a native module approach:

- Use `jazz-napi` (or a C FFI) compiled for iOS/Android via Hermes/JSI
- Native durable storage on device filesystem (expected to align with the SQLite-backed mobile runtime)
- Bridge between JS thread and native Rust runtime
- Same React hooks API as web (`react_bindings.md`) but backed by native storage

## Open Questions

- NAPI (via Hermes) vs. JSI (C++ bridge) vs. Turbo Modules?
- How to package pre-built Rust binaries for iOS (xcframework) and Android (JNI/NDK)?
- Background sync: can we keep syncing when the app is backgrounded?
- Expo plugin for zero-config setup?
- Shared code with web React bindings — how much can be reused?
