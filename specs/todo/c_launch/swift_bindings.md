# Swift Bindings — TODO

Swift client library for jazz2 on Apple platforms.

## Overview

Native Swift integration for iOS, macOS, watchOS, tvOS:

- Rust → C FFI → Swift wrapper (or UniFFI for auto-generated bindings)
- Swift-idiomatic API: `@Observable` classes, async/await, Combine publishers
- BfTree storage on device (Documents or Application Support directory)
- SwiftUI integration: property wrappers like `@Query` for reactive views
- SPM (Swift Package Manager) distribution with pre-built xcframeworks

## Open Questions

- UniFFI vs. hand-written C bridge vs. Swift-Bridge?
- How to expose reactive queries: Combine, AsyncSequence, or @Observable?
- Background sync via BGTaskScheduler?
- iCloud integration for cross-device sync (complementary to server sync)?
- Minimum deployment target (iOS 16+ for modern concurrency)?
