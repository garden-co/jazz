# Transaction FFI Optimization - Implementation Tasks (Unified `changes`)

## Overview

This task list updates the original plan to match the unified FFI transaction payload shape:
- **Single required `changes` string** for both `"private"` and `"trusting"` (encrypted vs JSON respectively)
- **Only optional fields**: `key_used` / `keyUsed` (required for `"private"`, absent for `"trusting"`) and `meta`
- **No `encrypted_changes` / `encryptedChanges`** in any public FFI transaction types

Each task references the relevant requirements from `requirements.md`.

---

## Tasks (numbered checklist)

1. **Core: keep `try_add_transactions` in cojson-core**
   - **File**: `crates/cojson-core/src/core/session_log.rs`
   - **Requirements**: US-1, US-4
   - [x] Ensure `try_add_transactions(Vec<Transaction>, ...)` remains the common entrypoint for all bindings

2. **WASM: unify `WasmFfiTransaction` to `changes`-only**
   - **File**: `crates/cojson-core-wasm/src/lib.rs`
   - **Requirements**: US-1, US-2, US-3
   - [ ] Update `WasmFfiTransaction` fields to: `privacy: String`, `changes: String`, `key_used: Option<String>`, `made_at: u64`, `meta: Option<String>`
   - [ ] Update `WasmFfiTransaction::new(...)` constructor to accept `changes: String` (and remove `encrypted_changes` / optional `changes`)
   - [ ] Update `to_transaction(...)`:
     - [ ] `"private"`: require `key_used`, map `changes` → `PrivateTransaction.encrypted_changes`
     - [ ] `"trusting"`: ignore/require-absent `key_used`, map `changes` → `TrustingTransaction.changes`
   - [ ] Update any WASM-facing docs/tests in this crate that refer to `encrypted_changes`

3. **NAPI: keep `createTransactionFfi` + `changes`-only record**
   - **Files**: `crates/cojson-core-napi/src/lib.rs`, `crates/cojson-core-napi/index.d.ts` (if applicable)
   - **Requirements**: US-1, US-2, US-3, US-4
   - [x] Ensure `NapiFfiTransaction` is `changes: String` + optional `key_used` + optional `meta`
   - [x] Ensure exported factory `createTransactionFfi(privacy, changes, key_used, made_at, meta)` enforces:
     - [x] `"trusting"` always produces `key_used: None`
   - [ ] Add/adjust NAPI tests to cover:
     - [ ] `"private"` requires `key_used`
     - [ ] `"trusting"` ignores any `key_used`

4. **React Native (uniffi): unify `UniffiFfiTransaction` to `changes`-only**
   - **File**: `crates/cojson-core-rn/rust/src/session_log.rs`
   - **Requirements**: US-1, US-2, US-3
   - [ ] Update `UniffiFfiTransaction` to fields: `privacy: String`, `changes: String`, `key_used: Option<String>`, `made_at: u64`, `meta: Option<String>`
   - [ ] Update `to_transaction(...)` mapping to match WASM/NAPI:
     - [ ] `"private"`: require `key_used`, map `changes` → encrypted changes
     - [ ] `"trusting"`: ignore/require-absent `key_used`, map `changes` → trusting changes

5. **React Native (uniffi): add a NAPI-style factory for ergonomic TS use**
   - **File**: `crates/cojson-core-rn/rust/src/session_log.rs`
   - **Requirements**: US-2, US-3
   - [ ] Export a uniffi function (e.g. `create_transaction_ffi` / `createTransactionFfi`) that constructs `UniffiFfiTransaction`
   - [ ] Enforce invariants in the factory:
     - [ ] Reject invalid `privacy`
     - [ ] `"trusting"` always returns `key_used: None`

6. **Regenerate RN TypeScript bindings after record/factory change**
   - **Files**: `crates/cojson-core-rn/*` (generated TS types)
   - **Requirements**: US-2, US-3
   - [ ] Regenerate uniffi RN TypeScript bindings so the updated `UniffiFfiTransaction` + factory are available
   - [ ] Verify the generated shape uses camelCase (`keyUsed`, `madeAt`, etc.) as expected by consumers

7. **TypeScript: unify helpers to match `changes`-only**
   - **File**: `packages/cojson/src/crypto/ffiTransaction.ts`
   - **Requirements**: US-1, US-2, US-3, US-4
   - [ ] Update `FfiTransactionObject` to require `changes: string` and remove `encryptedChanges`
   - [ ] Keep `toNapiFfiTransaction(...)` using `createTransactionFfi(...)` (NAPI-style)
   - [ ] Add a `toRnFfiTransaction(...)` that uses the RN exported factory (from task 5) instead of hand-rolled objects / casts
   - [ ] Update `toWasmFfiTransaction(...)` constructor call to match new WASM signature (task 2)

8. **TypeScript: update crypto adapters to use the new helpers**
   - **Files**: `packages/cojson/src/crypto/WasmCrypto.ts`, `packages/cojson/src/crypto/NapiCrypto.ts`, `packages/cojson/src/crypto/RNCrypto.ts`
   - **Requirements**: US-1, US-2, US-4
   - [ ] WASM: ensure adapter passes `transactions.map(toWasmFfiTransaction)` with the new ctor signature
   - [ ] RN: switch adapter to `transactions.map(toRnFfiTransaction)` and remove the `as any` escape hatch once bindings are regenerated

9. **Tests: update coverage for unified payload**
   - **Files**:
     - `crates/cojson-core-wasm/__test__/index.test.ts`
     - `crates/cojson-core-napi/__test__/index.test.ts`
     - `crates/cojson-core-rn/src/__tests__/index.test.tsx`
     - `packages/cojson/src/crypto/__tests__/ffiTransaction.test.ts` (if present/added)
   - **Requirements**: US-3, US-4
   - [ ] Add/adjust test cases that validate `changes` + `keyUsed` rules for both privacy modes
   - [ ] Compare `tryAdd` vs `tryAddFfi` behavior for equivalent inputs

10. **Benchmarks: validate performance goals**
    - **File**: `bench/transaction-ffi.ts`
    - **Requirements**: US-5
    - [ ] Benchmark `tryAdd` vs `tryAddFfi` for single + batch transactions
    - [ ] Confirm at least 2x speedup for single transactions on representative hardware
