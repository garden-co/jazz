# Transaction FFI Optimization - Implementation Tasks

## Overview

This document outlines the coding tasks required to implement the Transaction FFI optimization. Each task references the relevant requirements from `requirements.md`.

---

## Phase 1: Core Infrastructure

### Task 1.1: Add `try_add_transactions` method to SessionLogInternal
**File:** `crates/cojson-core/src/core/session_log.rs`
**Requirements:** US-1, US-4
**Status:** ✅ COMPLETED

- [x] Add new public method `try_add_transactions(transactions: Vec<Transaction>, signature: &Signature, skip_verify: bool)`
- [x] Refactor existing `try_add` to use shared internal logic (`try_add_internal`)
- [x] Ensure both methods produce identical results for equivalent inputs
- [x] Add `Encrypted::new()` constructor for creating encrypted fields

---

## Phase 2: WASM Binding

### Task 2.1: Create WasmFfiTransaction struct
**File:** `crates/cojson-core-wasm/src/lib.rs`
**Requirements:** US-1, US-2, US-3
**Status:** ✅ COMPLETED

- [x] Define `WasmFfiTransaction` struct with `#[wasm_bindgen(getter_with_clone)]`
- [x] Add constructor with `#[wasm_bindgen(constructor)]`
- [x] Fields: `privacy`, `encrypted_changes`, `key_used`, `changes`, `made_at`, `meta`

### Task 2.2: Implement to_transaction conversion function for WASM
**File:** `crates/cojson-core-wasm/src/lib.rs`
**Requirements:** US-1, US-3
**Status:** ✅ COMPLETED

- [x] Create `fn to_transaction(wasm: WasmFfiTransaction) -> Result<Transaction, CojsonCoreWasmError>`
- [x] Handle "private" case: validate `encrypted_changes` and `key_used` are present
- [x] Handle "trusting" case: validate `changes` is present
- [x] Return appropriate errors for missing fields or invalid privacy type

### Task 2.3: Add tryAddFfi method to SessionLog (WASM)
**File:** `crates/cojson-core-wasm/src/lib.rs`
**Requirements:** US-1, US-2
**Status:** ✅ COMPLETED

- [x] Add `try_add_ffi` method with `#[wasm_bindgen(js_name = tryAddFfi)]`
- [x] Accept `Vec<WasmFfiTransaction>`, convert using `to_transaction`
- [x] Call `internal.try_add_transactions()`

### Task 2.4: Add WASM integration tests
**File:** `crates/cojson-core-wasm/__test__/index.test.ts`
**Requirements:** US-1, US-4
**Status:** ⏳ PENDING

- [ ] Test `tryAddFfi` with private transactions
- [ ] Test `tryAddFfi` with trusting transactions
- [ ] Test error handling for missing fields
- [ ] Test error handling for invalid privacy type
- [ ] Compare results with existing `tryAdd` for equivalent inputs

---

## Phase 3: NAPI Binding

### Task 3.1: Create NapiFfiTransaction struct
**File:** `crates/cojson-core-napi/src/lib.rs`
**Requirements:** US-1, US-2, US-3
**Status:** ✅ COMPLETED

- [x] Define `NapiFfiTransaction` struct with `#[napi(object)]`
- [x] Fields: `privacy`, `encrypted_changes`, `key_used`, `changes`, `made_at`, `meta`

### Task 3.2: Implement to_transaction conversion function for NAPI
**File:** `crates/cojson-core-napi/src/lib.rs`
**Requirements:** US-1, US-3
**Status:** ✅ COMPLETED

- [x] Create `fn to_transaction(napi_tx: NapiFfiTransaction) -> Result<Transaction, napi::Error>`
- [x] Handle "private" and "trusting" cases with validation
- [x] Return `napi::Error` with `InvalidArg` status for validation failures

### Task 3.3: Add tryAddFfi method to SessionLog (NAPI)
**File:** `crates/cojson-core-napi/src/lib.rs`
**Requirements:** US-1, US-2
**Status:** ✅ COMPLETED

- [x] Add `try_add_ffi` method with `#[napi(js_name = "tryAddFfi")]`
- [x] Accept `Vec<NapiFfiTransaction>`, convert using `to_transaction`
- [x] Call `internal.try_add_transactions()`

### Task 3.4: Add NAPI integration tests
**File:** `crates/cojson-core-napi/__test__/index.test.ts`
**Requirements:** US-1, US-4
**Status:** ⏳ PENDING

- [ ] Test `tryAddFfi` with private transactions
- [ ] Test `tryAddFfi` with trusting transactions
- [ ] Test error handling for missing fields
- [ ] Compare results with existing `tryAdd`

---

## Phase 4: React Native / Uniffi Binding

### Task 4.1: Create UniffiFfiTransaction record
**File:** `crates/cojson-core-rn/rust/src/session_log.rs`
**Requirements:** US-1, US-2, US-3
**Status:** ✅ COMPLETED

- [x] Define `UniffiFfiTransaction` struct with `#[derive(uniffi::Record)]`
- [x] Fields: `privacy`, `encrypted_changes`, `key_used`, `changes`, `made_at`, `meta`

### Task 4.2: Implement to_transaction conversion function for Uniffi
**File:** `crates/cojson-core-rn/rust/src/session_log.rs`
**Requirements:** US-1, US-3
**Status:** ✅ COMPLETED

- [x] Create `fn to_transaction(uniffi_tx: UniffiFfiTransaction) -> Result<Transaction, SessionLogError>`
- [x] Handle "private" and "trusting" cases with validation
- [x] Return `SessionLogError::Generic` for validation failures

### Task 4.3: Add try_add_ffi method to SessionLog (Uniffi)
**File:** `crates/cojson-core-rn/rust/src/session_log.rs`
**Requirements:** US-1, US-2
**Status:** ✅ COMPLETED

- [x] Add `try_add_ffi` method with `#[uniffi::export]`
- [x] Accept `Vec<UniffiFfiTransaction>`, convert using `to_transaction`
- [x] Call `internal.try_add_transactions()` within lock

### Task 4.4: Regenerate Uniffi TypeScript bindings
**Requirements:** US-2, US-3
**Status:** ⏳ PENDING

- [ ] Run uniffi-bindgen-react-native to regenerate TypeScript types
- [ ] Verify `UniffiFfiTransaction` type is correctly generated

### Task 4.5: Add React Native integration tests
**File:** `crates/cojson-core-rn/src/__tests__/index.test.tsx`
**Requirements:** US-1, US-4
**Status:** ⏳ PENDING

- [ ] Test `tryAddFfi` with private transactions
- [ ] Test `tryAddFfi` with trusting transactions
- [ ] Test error handling

---

## Phase 5: TypeScript Integration

### Task 5.1: Create TypeScript FFI helpers
**File:** `packages/cojson/src/crypto/ffiTransaction.ts` (new file)
**Requirements:** US-1, US-3
**Status:** ✅ COMPLETED

- [x] Create `toWasmFfiTransaction(tx: Transaction): WasmFfiTransaction` function
- [x] Create `toFfiTransactionObject(tx: Transaction): FfiTransactionObject` for NAPI/RN
- [x] Create `toNapiFfiTransaction(tx: Transaction): NapiFfiTransaction` helper
- [x] Define `FfiTransactionObject` interface with camelCase fields

### Task 5.2: Update WasmCrypto SessionLogAdapter.tryAdd
**File:** `packages/cojson/src/crypto/WasmCrypto.ts`
**Requirements:** US-1, US-4
**Status:** ✅ COMPLETED

- [x] Import `toWasmFfiTransaction` from ffiTransaction.ts
- [x] Update `tryAdd` method to use `toWasmFfiTransaction` and `tryAddFfi`
- [x] Remove `JSON.stringify` call

### Task 5.3: Update NapiCrypto SessionLogAdapter.tryAdd
**File:** `packages/cojson/src/crypto/NapiCrypto.ts`
**Requirements:** US-1, US-4
**Status:** ✅ COMPLETED

- [x] Import `toNapiFfiTransaction` from ffiTransaction.ts
- [x] Update `tryAdd` method to use `toNapiFfiTransaction` and `tryAddFfi`
- [x] Remove `JSON.stringify` call

### Task 5.4: Update RNCrypto SessionLogAdapter.tryAdd
**File:** `packages/cojson/src/crypto/RNCrypto.ts`
**Requirements:** US-1, US-4
**Status:** ✅ COMPLETED

- [x] Import `toFfiTransactionObject` from ffiTransaction.ts
- [x] Update `tryAdd` method to use `toFfiTransactionObject` and `tryAddFfi`
- [x] Remove `JSON.stringify` call
- [x] Use `as any` cast for TypeScript compatibility until bindings regenerated

### Task 5.5: Add TypeScript unit tests
**File:** `packages/cojson/src/crypto/__tests__/ffiTransaction.test.ts` (new file)
**Requirements:** US-3, US-4
**Status:** ⏳ PENDING

- [ ] Test `toWasmFfiTransaction` for private transactions
- [ ] Test `toWasmFfiTransaction` for trusting transactions
- [ ] Test `toFfiTransactionObject` for both transaction types

---

## Phase 6: Performance Validation

### Task 6.1: Create benchmark for tryAdd vs tryAddFfi
**File:** `bench/transaction-ffi.ts` (new file)
**Requirements:** US-5
**Status:** ⏳ PENDING

- [ ] Benchmark single transaction: `tryAdd` vs `tryAddFfi`
- [ ] Benchmark batch (10, 100, 1000 transactions)
- [ ] Measure memory allocation if possible
- [ ] Document results

### Task 6.2: Validate 2x performance improvement
**Requirements:** US-5
**Status:** ⏳ PENDING

- [ ] Run benchmarks on representative hardware
- [ ] Verify at least 2x improvement for single transactions
- [ ] Document any edge cases or limitations

---

## Summary

| Phase | Tasks | Completed | Pending |
|-------|-------|-----------|---------|
| Phase 1: Core | 1 | 1 | 0 |
| Phase 2: WASM | 4 | 3 | 1 |
| Phase 3: NAPI | 4 | 3 | 1 |
| Phase 4: RN/Uniffi | 5 | 3 | 2 |
| Phase 5: TypeScript | 5 | 4 | 1 |
| Phase 6: Benchmarks | 2 | 0 | 2 |
| **Total** | **21** | **14** | **7** |

## Task Dependencies

```
Phase 1 (Core) ✅
    │
    ├──► Phase 2 (WASM) ✅ ──► Tests ⏳
    │
    ├──► Phase 3 (NAPI) ✅ ──► Tests ⏳
    │
    └──► Phase 4 (Uniffi/RN) ✅ ──► Bindings Regen ⏳ ──► Tests ⏳
                                          │
                                          ▼
                                   Phase 5 (TypeScript) ✅ ──► Tests ⏳
                                          │
                                          ▼
                                   Phase 6 (Benchmarks) ⏳
```

- ✅ Core implementation complete across all platforms
- ⏳ Integration tests and benchmarks pending
- ⏳ Uniffi TypeScript bindings regeneration pending
