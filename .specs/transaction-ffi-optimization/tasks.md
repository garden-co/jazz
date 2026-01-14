# Transaction FFI Optimization - Implementation Tasks

## Overview

This task list reflects the unified FFI transaction payload shape:
- **Single required `changes` string** for both `"private"` and `"trusting"` (encrypted vs JSON respectively)
- **Only optional fields**: `key_used` / `keyUsed` (required for `"private"`, absent for `"trusting"`) and `meta`
- **No `encrypted_changes` / `encryptedChanges`** in any public FFI transaction types
- **No shared helper file**: Each crypto adapter defines its own inline conversion function

Each task references the relevant requirements from `requirements.md`.

---

## Phase 1: Core Infrastructure

### Task 1.1: Maintain `try_add_transactions` method in SessionLogInternal
**File:** `crates/cojson-core/src/core/session_log.rs`
**Requirements:** US-1, US-4
**Status:** ✅ COMPLETED

- [x] Ensure `try_add_transactions(Vec<Transaction>, ...)` remains the common entrypoint for all bindings
- [x] `Encrypted::new()` constructor available for creating encrypted fields

---

## Phase 2: WASM Binding

### Task 2.1: Unify WasmFfiTransaction to changes-only
**File:** `crates/cojson-core-wasm/src/lib.rs`
**Requirements:** US-1, US-2, US-3
**Status:** ✅ COMPLETED

- [x] Update `WasmFfiTransaction` fields to: `privacy: String`, `key_used: Option<String>`, `changes: String`, `made_at: u64`, `meta: Option<String>`
- [x] Update `WasmFfiTransaction::new(...)` constructor signature to `(privacy, key_used, changes, made_at, meta)`
- [x] Update `to_transaction(...)`:
  - [x] `"private"`: require `key_used`, map `changes` → `PrivateTransaction.encrypted_changes`
  - [x] `"trusting"`: map `changes` → `TrustingTransaction.changes`

### Task 2.2: Update WasmCrypto.ts inline conversion
**File:** `packages/cojson/src/crypto/WasmCrypto.ts`
**Requirements:** US-1, US-4
**Status:** ✅ COMPLETED

- [x] Define inline `toWasmFfiTransaction(tx: Transaction)` function
- [x] Use `new WasmFfiTransaction(privacy, keyUsed, changes, madeAt, meta)` constructor
- [x] Update `SessionLogAdapter.tryAdd` to use `transactions.map(toWasmFfiTransaction)`

### Task 2.3: Add WASM integration tests
**File:** `crates/cojson-core-wasm/__test__/index.test.ts`
**Requirements:** US-1, US-4
**Status:** ⏳ PENDING

- [ ] Test `tryAddFfi` with private transactions
- [ ] Test `tryAddFfi` with trusting transactions
- [ ] Test error handling for missing `key_used` on private
- [ ] Test error handling for invalid privacy type
- [ ] Compare results with existing `tryAdd` for equivalent inputs

---

## Phase 3: NAPI Binding

### Task 3.1: Maintain NapiFfiTransaction + createTransactionFfi factory
**File:** `crates/cojson-core-napi/src/lib.rs`
**Requirements:** US-1, US-2, US-3
**Status:** ✅ COMPLETED

- [x] `NapiFfiTransaction` has fields: `privacy: String`, `changes: String`, `key_used: Option<String>`, `made_at: BigInt`, `meta: Option<String>`
- [x] Exported factory `createTransactionFfi(privacy, changes, key_used, made_at, meta)` returns `NapiFfiTransaction`
- [x] `to_transaction(...)` maps correctly for both privacy modes

### Task 3.2: Update NapiCrypto.ts inline conversion
**File:** `packages/cojson/src/crypto/NapiCrypto.ts`
**Requirements:** US-1, US-4
**Status:** ✅ COMPLETED

- [x] Define inline `toNapiFfiTransaction(tx: Transaction)` function
- [x] Use `createTransactionFfi(privacy, changes, keyUsed, madeAt, meta)` factory
- [x] Update `SessionLogAdapter.tryAdd` to use `transactions.map(toNapiFfiTransaction)`

### Task 3.3: Add NAPI integration tests
**File:** `crates/cojson-core-napi/__test__/index.test.ts`
**Requirements:** US-1, US-4
**Status:** ⏳ PENDING

- [ ] Test `tryAddFfi` with private transactions
- [ ] Test `tryAddFfi` with trusting transactions
- [ ] Test error handling for missing `key_used` on private
- [ ] Compare results with existing `tryAdd`

---

## Phase 4: React Native / Uniffi Binding

### Task 4.1: Unify UniffiFfiTransaction to changes-only
**File:** `crates/cojson-core-rn/rust/src/session_log.rs`
**Requirements:** US-1, US-2, US-3
**Status:** ✅ COMPLETED

- [x] Update `UniffiFfiTransaction` fields to: `privacy: String`, `key_used: Option<String>`, `changes: String`, `made_at: u64`, `meta: Option<String>`
- [x] Update `to_transaction(...)`:
  - [x] `"private"`: require `key_used`, map `changes` → encrypted changes
  - [x] `"trusting"`: map `changes` → trusting changes

### Task 4.2: Add create_transaction_ffi factory (NAPI-style)
**File:** `crates/cojson-core-rn/rust/src/session_log.rs`
**Requirements:** US-2, US-3
**Status:** ✅ COMPLETED

- [x] Export `#[uniffi::export] fn create_transaction_ffi(privacy, changes, key_used, made_at, meta) -> UniffiFfiTransaction`
- [x] Factory constructs record directly (validation happens in `to_transaction`)

### Task 4.3: Regenerate Uniffi TypeScript bindings
**File:** `crates/cojson-core-rn/src/generated/cojson_core_rn.ts`
**Requirements:** US-2, US-3
**Status:** ✅ COMPLETED

- [x] Updated `UniffiFfiTransaction` type to use `changes: string` (required), `keyUsed?: string`, `madeAt: bigint`, `meta?: string`
- [x] Added `createTransactionFfi(...)` export to generated bindings
- [x] Verified camelCase field names match expectations

### Task 4.4: Update RNCrypto.ts inline conversion
**File:** `packages/cojson/src/crypto/RNCrypto.ts`
**Requirements:** US-1, US-4
**Status:** ✅ COMPLETED

- [x] Define inline `toUniffiFfiTransaction(tx: Transaction)` function
- [x] Use `createTransactionFfi(privacy, changes, keyUsed, madeAt, meta)` from `cojson-core-rn`
- [x] Update `SessionLogAdapter.tryAdd` to use `transactions.map(toUniffiFfiTransaction)`
- [x] Remove `as any` escape hatch (bindings regenerated)

### Task 4.5: Add React Native integration tests
**File:** `crates/cojson-core-rn/src/__tests__/index.test.tsx`
**Requirements:** US-1, US-4
**Status:** ⏳ PENDING

- [ ] Test `tryAddFfi` with private transactions
- [ ] Test `tryAddFfi` with trusting transactions
- [ ] Test error handling

---

## Phase 5: Cleanup

### Task 5.1: Remove shared ffiTransaction.ts helper (if present)
**File:** `packages/cojson/src/crypto/ffiTransaction.ts`
**Requirements:** US-4
**Status:** ✅ COMPLETED

- [x] File deleted – each crypto adapter now has its own inline conversion function

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
| Phase 2: WASM | 3 | 2 | 1 |
| Phase 3: NAPI | 3 | 2 | 1 |
| Phase 4: RN/Uniffi | 5 | 4 | 1 |
| Phase 5: Cleanup | 1 | 1 | 0 |
| Phase 6: Benchmarks | 2 | 0 | 2 |
| **Total** | **15** | **10** | **5** |

## Task Dependencies

```
Phase 1 (Core) ✅
    │
    ├──► Phase 2 (WASM) ✅ ──► Tests ⏳
    │
    ├──► Phase 3 (NAPI) ✅ ──► Tests ⏳
    │
    └──► Phase 4 (Uniffi/RN) ✅ ──► Tests ⏳
                │
                ▼
         Phase 5 (Cleanup) ✅
                │
                ▼
         Phase 6 (Benchmarks) ⏳
```

**Implementation Status:**
- ✅ Core FFI optimization complete across all platforms (WASM, NAPI, RN)
- ✅ Unified `changes` field implemented everywhere
- ✅ Factory functions (`createTransactionFfi`) available for NAPI and RN
- ✅ Inline conversion functions in each crypto adapter (no shared helper)
- ⏳ Integration tests pending
- ⏳ Performance benchmarks pending
