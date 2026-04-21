# React Native Packaging — TODO

React Native support now exists through the `jazz-rn` Turbo Module and the
`packages/jazz-tools/src/react-native` adapter surface. The remaining launch
work is packaging hardening, ergonomics, and operational clarity.

## Overview

React Native still cannot use OPFS or Web Workers, so it follows a separate
native-runtime path:

- `crates/jazz-rn/` provides a UniFFI-backed Turbo Module for iOS and Android
- `packages/jazz-tools/src/react-native/` exposes `createDb`, `createJazzClient`, and the RN runtime adapter
- Local durability uses the native embedded storage backend instead of browser APIs
- The React-facing API aims to stay close to the web bindings, but uses RN-specific runtime plumbing underneath

## Open Questions

- How to package pre-built Rust binaries for iOS (xcframework) and Android (JNI/NDK)?
- Background sync: can we keep syncing when the app is backgrounded?
- Expo plugin for zero-config setup?
- Shared code with web React bindings — how much can be reused?
