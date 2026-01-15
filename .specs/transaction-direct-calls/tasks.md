# Transaction Direct Calls - Implementation Tasks

## Overview

This document outlines the coding tasks to optimize `tryAdd` by replacing JSON serialization with direct FFI calls.

**Status: ✅ COMPLETED**

---

## Phase 1: Core Layer (`cojson-core`)

### Task 1.1: Add `pending_transactions` field to `SessionLogInternal`
**File:** `crates/cojson-core/src/core/session_log.rs`

- [x] Add `pending_transactions: Vec<String>` field to `SessionLogInternal` struct
- [x] Initialize `pending_transactions: Vec::new()` in `SessionLogInternal::new()`

### Task 1.2: Implement `add_existing_private_transaction`
**File:** `crates/cojson-core/src/core/session_log.rs`

- [x] Add method that builds `PrivateTransaction` and adds to `pending_transactions`
- [x] Return `Result<(), CoJsonCoreError>` with proper error handling
- [x] On error: clear pending and propagate

### Task 1.3: Implement `add_existing_trusting_transaction`
**File:** `crates/cojson-core/src/core/session_log.rs`

- [x] Add method that builds `TrustingTransaction` and adds to `pending_transactions`
- [x] Return `Result<(), CoJsonCoreError>` with proper error handling
- [x] On error: clear pending and propagate

### Task 1.4: Implement `commit_transactions` with staging area
**File:** `crates/cojson-core/src/core/session_log.rs`

- [x] Compute hash over `committed + pending` using cloned hasher
- [x] If `skip_validate` is true: commit directly without signature check
- [x] If `skip_validate` is false: validate signature, then commit on success or clear pending on failure

### Task 1.5: Implement helper methods
**File:** `crates/cojson-core/src/core/session_log.rs`

- [x] Add `has_pending()` method

---

## Phase 2: NAPI Binding (`cojson-core-napi`)

### Task 2.1: Add bindings for direct call methods
**File:** `crates/cojson-core-napi/src/lib.rs`

- [x] Add `addExistingPrivateTransaction` with `BigInt` for `madeAt`
- [x] Add `addExistingTrustingTransaction` with `BigInt` for `madeAt`
- [x] Add `commitTransactions` with `signature` and `skip_validate` parameters

---

## Phase 3: WASM Binding (`cojson-core-wasm`)

### Task 3.1: Add bindings for direct call methods
**File:** `crates/cojson-core-wasm/src/lib.rs`

- [x] Add `addExistingPrivateTransaction` with `u64` for `madeAt`
- [x] Add `addExistingTrustingTransaction` with `u64` for `madeAt`
- [x] Add `commitTransactions` with `signature` and `skip_validate` parameters

---

## Phase 4: Uniffi Binding (`cojson-core-rn`)

### Task 4.1: Add bindings for direct call methods
**File:** `crates/cojson-core-rn/rust/src/session_log.rs`

- [x] Add `add_existing_private_transaction` with `u64` for `made_at`
- [x] Add `add_existing_trusting_transaction` with `u64` for `made_at`
- [x] Add `commit_transactions` with `signature` and `skip_validate` parameters

---

## Phase 5: TypeScript Integration

### Task 5.1: Update `tryAdd` in NapiCrypto.ts
**File:** `packages/cojson/src/crypto/NapiCrypto.ts`

- [x] Replace `JSON.stringify` with loop over transactions
- [x] Call `addExistingPrivateTransaction` for private transactions
- [x] Call `addExistingTrustingTransaction` for trusting transactions
- [x] Call `commitTransactions(signature, skipVerify)` at the end

### Task 5.2: Update `tryAdd` in WasmCrypto.ts
**File:** `packages/cojson/src/crypto/WasmCrypto.ts`

- [x] Replace `JSON.stringify` with loop over transactions
- [x] Call `addExistingPrivateTransaction` for private transactions
- [x] Call `addExistingTrustingTransaction` for trusting transactions
- [x] Call `commitTransactions(signature, skipVerify)` at the end

### Task 5.3: Update `tryAdd` in RNCrypto.ts
**File:** `packages/cojson/src/crypto/RNCrypto.ts`

- [x] Replace `JSON.stringify` with loop over transactions
- [x] Call `addExistingPrivateTransaction` for private transactions
- [x] Call `addExistingTrustingTransaction` for trusting transactions
- [x] Call `commitTransactions(signature, skipVerify)` at the end

---

## Phase 6: Build & Verify

### Task 6.1: Verify Rust compilation
- [x] Run `cargo check` in `crates/cojson-core`
- [x] Run `cargo check` in `crates/cojson-core-napi`
- [x] Run `cargo check` in `crates/cojson-core-wasm`
- [x] Run `cargo check` in `crates/cojson-core-rn/rust`
- [x] All crates compile successfully

### Task 6.2: Verify TypeScript compilation
- [ ] TypeScript types for `cojson-core-rn` are auto-generated during build
- [ ] Full build required to regenerate UniFFI bindings

---

## Phase 7: Benchmarking

### Task 7.1: Create performance benchmark
**File:** `bench/cojson/transaction-direct-calls.bench.ts`

- [x] Create benchmark using cronometro
- [x] Compare OLD approach (JSON.stringify) vs NEW approach (direct calls)
- [x] Test with private, trusting, and mixed transactions
- [x] Test with varying transaction counts (10, 100, 500)

### Task 7.2: Add benchmark script
**File:** `bench/package.json`

- [x] Add `bench:direct-calls` script to run the benchmark

---

## Summary

| Phase | Tasks | Status |
|-------|-------|--------|
| 1. Core Layer | 5 tasks | ✅ Complete |
| 2. NAPI Binding | 1 task | ✅ Complete |
| 3. WASM Binding | 1 task | ✅ Complete |
| 4. Uniffi Binding | 1 task | ✅ Complete |
| 5. TypeScript Integration | 3 tasks | ✅ Complete |
| 6. Build & Verify | 2 tasks | ⏳ Pending build |
| 7. Benchmarking | 2 tasks | ✅ Complete |
| **Total** | **15 tasks** | **✅ Code Complete** |

## API Summary

### Direct Call Methods

| Method | Parameters | Description |
|--------|------------|-------------|
| `addExistingPrivateTransaction` | `encrypted_changes`, `key_used`, `made_at`, `meta` | Stage a private transaction |
| `addExistingTrustingTransaction` | `changes`, `made_at`, `meta` | Stage a trusting transaction |
| `commitTransactions` | `signature`, `skip_validate` | Commit staged transactions |

### TypeScript Usage

```typescript
tryAdd(transactions, signature, skipVerify) {
  for (const tx of transactions) {
    if (tx.privacy === "private") {
      this.sessionLog.addExistingPrivateTransaction(...);
    } else {
      this.sessionLog.addExistingTrustingTransaction(...);
    }
  }
  this.sessionLog.commitTransactions(signature, skipVerify);
}
```

## Files Modified

### Rust
- `crates/cojson-core/src/core/session_log.rs` - Core implementation with staging area
- `crates/cojson-core-napi/src/lib.rs` - NAPI binding
- `crates/cojson-core-wasm/src/lib.rs` - WASM binding
- `crates/cojson-core-rn/rust/src/session_log.rs` - Uniffi binding

### TypeScript
- `packages/cojson/src/crypto/NapiCrypto.ts` - NAPI adapter with optimized `tryAdd`
- `packages/cojson/src/crypto/WasmCrypto.ts` - WASM adapter with optimized `tryAdd`
- `packages/cojson/src/crypto/RNCrypto.ts` - React Native adapter with optimized `tryAdd`

### Benchmarks
- `bench/cojson/transaction-direct-calls.bench.ts` - Performance benchmark comparing approaches
- `bench/package.json` - Added `bench:direct-calls` script
