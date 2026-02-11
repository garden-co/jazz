# React Native Storage Investigation — TODO

**Owner: Antonio**

De-risk synchronous storage and BfTree on React Native.

## Overview

React Native can't use OPFS or Web Workers. The first step is proving that BfTree's synchronous storage API works on mobile:

- Compile BfTree for iOS (xcframework) and Android (JNI/NDK)
- Wire up filesystem access (Documents directory on iOS, internal storage on Android)
- Get basic read/write/flush working from a React Native app
- Don't worry about code cleanliness or commonality with WASM/NAPI yet — just make it work

This is explicitly a spike / proof-of-concept. The full React Native packaging story is in `../c_launch/react_native_packaging.md`.

## Success Criteria

- BfTree opens, writes, reads, and flushes on both iOS simulator and Android emulator
- A minimal RN app can store and retrieve data through the Rust FFI
