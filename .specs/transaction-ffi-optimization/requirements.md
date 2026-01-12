# Transaction FFI Optimization

## Introduction

The current implementation of `SessionLogAdapter.tryAdd()` in `WasmCrypto.ts` serializes each transaction to JSON using `JSON.stringify()` before passing it to Rust. On the Rust side, the JSON is then parsed back into a `Transaction` struct. This double serialization is a performance bottleneck, especially for large transaction batches.

The goal is to eliminate JSON serialization by passing transaction data directly to Rust using FFI-compatible structs. This optimization must work consistently across all three binding environments:
- **WASM** (browser/Node.js via `wasm-bindgen`)
- **NAPI** (Node.js native via `napi-rs`)
- **React Native** (mobile via `uniffi-bindgen-react-native`)

## User Stories

### US-1: Zero-Copy Transaction Passing
**As a** Jazz application developer  
**I want** transactions to be passed to Rust without JSON serialization  
**So that** transaction processing is faster and uses less memory

**Acceptance Criteria:**
- The `tryAdd` function accepts structured transaction objects instead of JSON strings
- No `JSON.stringify()` is called in the TypeScript layer for transaction data
- No `serde_json::from_str()` is called in the Rust layer for incoming transactions
- All existing functionality remains intact (signature verification, hashing, etc.)

### US-2: Cross-Platform Compatibility
**As a** Jazz framework maintainer  
**I want** the Transaction FFI types to work across all binding environments  
**So that** I can maintain a single implementation that works everywhere

**Acceptance Criteria:**
- The FFI Transaction types work with `wasm-bindgen` for WASM builds
- The FFI Transaction types work with `napi-rs` for Node.js native builds
- The FFI Transaction types work with `uniffi` for React Native builds
- The TypeScript interface is consistent across all platforms
- No platform-specific code is needed in application code

### US-3: Type Safety
**As a** Jazz application developer  
**I want** the FFI transaction types to be type-safe in TypeScript  
**So that** I get compile-time errors for invalid transaction structures

**Acceptance Criteria:**
- TypeScript types are generated/available for the FFI Transaction structs
- The TypeScript types match the existing `Transaction`, `PrivateTransaction`, and `TrustingTransaction` types
- Invalid transaction structures cause TypeScript compilation errors
- Runtime validation exists for edge cases (e.g., missing required fields)

### US-4: Backward Compatibility
**As a** Jazz application developer  
**I want** existing code to continue working during the migration  
**So that** I don't have to update my entire codebase at once

**Acceptance Criteria:**
- Existing JSON-based API remains functional during transition (if needed)
- No breaking changes to the public API of `SessionLogAdapter`
- The `Transaction`, `PrivateTransaction`, and `TrustingTransaction` TypeScript types remain unchanged
- All existing tests pass without modification

### US-5: Performance Improvement
**As a** Jazz application developer  
**I want** measurable performance improvement in transaction processing  
**So that** my application feels more responsive

**Acceptance Criteria:**
- Transaction processing is at least 2x faster for single transactions
- Batch transaction processing (`tryAdd` with multiple transactions) shows proportional improvement
- Memory allocation is reduced (no intermediate JSON strings)
- Performance improvement is validated with benchmarks
