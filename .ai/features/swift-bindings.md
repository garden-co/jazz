---
name: swift-bindings
description: Complete Swift binding layer for Jazz database on Apple platforms
status: active
files:
  - crates/jazz-swift/
tests:
  - crates/jazz-swift/rust/src/lib.rs
  - crates/jazz-swift/Tests/JazzSwiftBindingsTests/JazzSwiftBindingsTests.swift
coverage: partial
added_in: 7d42050f
last_verified: 7d42050f
created_at: 1775868990Z
updated_at: 1775868990Z
---

# jazz-swift

Thin Apple-platform binding surface for Jazz.

Provides UniFFI-based Rust-to-Swift bindings that mirror the jazz-rn surface for runtime bootstrap, queries, subscriptions, and sync operations.
