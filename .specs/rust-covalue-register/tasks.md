# Rust CoValue Register - Implementation Tasks

## Overview

This task list follows the phased migration strategy from the design document. Each task references specific requirements and includes acceptance criteria.

---

## Phase 1: Infrastructure & Core Data Structures

### Task 1.1: Create `CoValueRegister` skeleton in `cojson-core` ✅
**References**: US-1, US-5

- [x] Create `crates/cojson-core/src/core/register.rs`
- [x] Define `CoValueRegister` struct with `HashMap<String, CoValueHeader>` and `HashMap<String, SessionMapInternal>`
- [x] Implement `new()` and `with_capacity()` constructors
- [x] Implement `size()` method
- [x] Add module to `crates/cojson-core/src/lib.rs`
- [x] Add unit tests for basic lifecycle

### Task 1.2: Define `CoValueHeader` data structures ✅
**References**: US-8

- [x] Define `CoValueHeader` struct matching TypeScript format
- [x] Define `RulesetDef` enum with `Group`, `OwnedByGroup`, `UnsafeAllowAll` variants
- [x] Define `Uniqueness` enum (string, bool, null, integer, object)
- [x] Add serde serialization with proper camelCase field names
- [x] Add unit tests for JSON serialization/deserialization roundtrip

### Task 1.3: Define `RegisterError` type ✅
**References**: NFR-3

- [x] Create `RegisterError` enum in `crates/cojson-core/src/core/register.rs` (included in register module)
- [x] Add variants: `NotFound`, `HeaderExists`, `InvalidHeader`, `SignatureVerification`, `Decryption`, `Serialization`
- [x] Implement `thiserror::Error` derive

### Task 1.4: Define `KnownState` and related types ✅
**References**: US-7

- [x] Define `KnownState` struct with `id`, `header`, `sessions` fields
- [x] Add serde serialization
- [x] Add unit tests

---

## Phase 2: Header Storage

### Task 2.1: Implement header operations in `CoValueRegister` ✅
**References**: US-2

- [x] Implement `set_header(&mut self, id: &str, header: CoValueHeader) -> Result<(), RegisterError>`
- [x] Implement `get_header(&self, id: &str) -> Option<CoValueHeader>`
- [x] Implement `has_header(&self, id: &str) -> bool`
- [x] Add validation that header doesn't already exist
- [x] Add unit tests for all header operations

### Task 2.2: Implement memory management ✅
**References**: US-4

- [x] Implement `free(&mut self, id: &str) -> bool`
- [x] Implement `free_all(&mut self)`
- [x] Ensure `free` removes both header and session map
- [x] Add unit tests for lifecycle management

### Task 2.3: Add NAPI bindings for header operations ✅
**References**: US-5

- [x] Create `crates/cojson-core-napi/src/register.rs`
- [x] Define `Register` wrapper struct
- [x] Implement `#[napi(constructor)]` for `new()`
- [x] Implement `#[napi] set_header(id: String, header_json: String)`
- [x] Implement `#[napi] get_header(id: String) -> Option<String>`
- [x] Implement `#[napi] has_header(id: String) -> bool`
- [x] Implement `#[napi] free(id: String) -> bool`
- [x] Implement `#[napi] free_all()`
- [x] Implement `#[napi] size() -> u32`
- [x] Add module to `crates/cojson-core-napi/src/lib.rs`
- [x] Add NAPI tests in `crates/cojson-core-napi/__test__/register.test.ts`

### Task 2.4: Add WASM bindings for header operations ✅
**References**: US-5

- [x] Create `crates/cojson-core-wasm/src/register.rs`
- [x] Define `Register` wrapper struct with `#[wasm_bindgen]`
- [x] Implement constructor and header methods
- [x] Add module to `crates/cojson-core-wasm/src/lib.rs`
- [x] Update TypeScript declarations (auto-generated during build)
- [x] Add WASM tests in `crates/cojson-core-wasm/__test__/register.test.ts`

### Task 2.5: Add React Native bindings for header operations ✅
**References**: US-5

- [x] Create `crates/cojson-core-rn/rust/src/register.rs` with UniFFI bindings
- [x] Update `crates/cojson-core-rn/rust/src/lib.rs` with Register exports
- [x] Define `RegisterUniFFIError` enum for error handling
- [ ] Regenerate bindings (requires native build: `pnpm build:rn`)
- [ ] Add tests (after bindings regeneration)

---

## Phase 3: Session Map & Transaction Storage

### Task 3.1: Define `SessionMapInternal` struct ✅
**References**: US-3, US-6

- [x] Create `SessionMapInternal` struct in `register.rs`
- [x] Include `co_id: String`, `sessions: HashMap<String, SessionLogInternal>`, `known_state: KnownState`
- [x] Add `known_state_with_streaming: Option<KnownState>` and `is_deleted: bool`

### Task 3.2: Implement session map operations in `CoValueRegister` ✅
**References**: US-3

- [x] Implement `create_session_map(&mut self, id: &str) -> Result<(), RegisterError>`
- [x] Implement `get_or_create_session(&mut self, id: &str, session_id: &str, signer_id: Option<&str>) -> Result<(), RegisterError>`
- [x] Implement `get_session_tx_count(&self, id: &str, session_id: &str) -> u32`
- [x] Add unit tests

### Task 3.3: Implement transaction operations ✅
**References**: US-3, US-6

- [x] Implement `add_transactions()` that delegates to `SessionLogInternal`
- [x] Support both private and trusting transactions
- [x] Handle signature verification via `commit_transactions`
- [x] Add unit tests with test fixtures

### Task 3.4: Define `RawTransactionData` and query options ✅
**References**: US-9

- [x] Define `RawTransactionData` struct with all transaction fields
- [x] Define `RawTransactionQueryOptions` struct
- [x] Add serde serialization

### Task 3.5: Add NAPI bindings for session operations ✅
**References**: US-5

- [x] Add `create_session_map(id: String)` to Register
- [x] Add `add_transactions(id, session_id, signer_id, transactions_json, signature, skip_verify)`
- [x] Add `get_session_tx_count(id, session_id)`
- [x] Add tests

### Task 3.6: Add WASM bindings for session operations ✅
**References**: US-5

- [x] Mirror NAPI bindings for WASM
- [x] Add tests

### Task 3.7: Add React Native bindings for session operations ✅
**References**: US-5

- [x] Add session map and transaction methods to Register (in register.rs)
- [ ] Regenerate bindings (requires native build: `pnpm build:rn`)
- [ ] Add tests (after bindings regeneration)

---

## Phase 4: Known State & Batch Queries

### Task 4.1: Implement known state tracking ✅
**References**: US-7

- [x] Implement `get_known_state(&self, id: &str) -> Result<KnownState, RegisterError>`
- [x] Implement `get_known_state_with_streaming(&self, id: &str) -> Result<KnownState, RegisterError>`
- [x] Auto-update known state when transactions are added
- [x] Add unit tests

### Task 4.2: Implement `get_raw_transactions` batch query ✅
**References**: US-9, NFR-1

- [x] Implement `get_raw_transactions(&self, id, options) -> Result<Vec<RawTransactionData>, RegisterError>`
- [x] Support `from_sessions` and `to_sessions` filtering
- [x] Return all transaction data in a single call
- [x] Add unit tests with various filter combinations

### Task 4.3: Add NAPI bindings for known state and batch queries ✅
**References**: US-5

- [x] Add `get_known_state(id: String) -> String` (JSON)
- [x] Add `get_known_state_with_streaming(id: String) -> String`
- [x] Add `get_raw_transactions(id: String, options_json: String) -> String`
- [x] Add tests

### Task 4.4: Add WASM/RN bindings for known state and batch queries ✅
**References**: US-5

- [x] Mirror NAPI bindings for WASM (done)
- [x] Add WASM tests
- [x] Add known state and batch query methods to RN Register (in register.rs)
- [ ] Regenerate RN bindings (requires native build: `pnpm build:rn`)
- [ ] Add React Native tests (after bindings regeneration)

---

## Phase 5: `newContentSince` Algorithm

### Task 5.1: Define `ContentMessage` and `SessionContent` structs ✅
**References**: US-9

- [x] Define `ContentMessage` struct matching TypeScript `NewContentMessage`
- [x] Define `SessionContent` struct with `after`, `transactions`, `last_signature`
- [x] Add serde serialization with proper field names

### Task 5.2: Implement `get_content_since` algorithm in Rust ✅
**References**: US-9, NFR-1

- [x] Implement `get_content_since(&self, id, known_state) -> Result<Vec<ContentMessage>, RegisterError>`
- [x] Port iteration logic from TypeScript `verifiedState.ts:newContentSince()`
- [x] Include header if not known
- [x] Build session content messages for transactions after known state
- [x] Handle `expectContentUntil` for streaming state
- [x] Add comprehensive unit tests

### Task 5.3: Add NAPI bindings for `getContentSince` ✅
**References**: US-5

- [x] Add `get_content_since(id: String, known_state_json: Option<String>) -> String`
- [x] Return JSON array of ContentMessage
- [x] Add tests comparing output with TypeScript implementation

### Task 5.4: Add WASM/RN bindings for `getContentSince` ✅
**References**: US-5

- [x] Mirror NAPI bindings for WASM (done)
- [x] Add WASM tests
- [x] Add `get_content_since` to RN Register (in register.rs)
- [ ] Regenerate RN bindings (requires native build: `pnpm build:rn`)
- [ ] Add React Native tests (after bindings regeneration)

---

## Phase 6: Decryption Support

### Task 6.1: Implement decryption methods in Register ✅
**References**: Design section on decryption

- [x] Implement `decrypt_transaction(&self, id, session_id, tx_index, key_secret) -> Result<Option<String>, RegisterError>`
- [x] Implement `decrypt_transaction_meta(&self, id, session_id, tx_index, key_secret) -> Result<Option<String>, RegisterError>`
- [x] Delegate to `SessionLogInternal` decrypt methods
- [ ] Add unit tests

### Task 6.2: Add NAPI bindings for decryption ✅
**References**: US-5

- [x] Add `decrypt_transaction(id, session_id, tx_index, key_secret) -> Option<String>`
- [x] Add `decrypt_transaction_meta(id, session_id, tx_index, key_secret) -> Option<String>`
- [ ] Add tests

### Task 6.3: Add WASM/RN bindings for decryption ✅
**References**: US-5

- [x] Mirror NAPI bindings for WASM (done)
- [x] Add WASM tests (included in register.test.ts)
- [x] Add decryption methods to RN Register (in register.rs)
- [ ] Regenerate RN bindings (requires native build: `pnpm build:rn`)
- [ ] Add React Native tests (after bindings regeneration)

---

## Phase 7: TypeScript Integration ✅

### Task 7.1: Create TypeScript wrapper for Rust Register ✅
**References**: Design - TypeScript Integration

- [x] Create `packages/cojson/src/coValueCore/rustRegister.ts`
- [x] Define `RustRegister` class wrapping native bindings
- [x] Implement type-safe methods with JSON parsing
- [x] Add method `getContentSince()` returning parsed content messages
- [x] Add `NativeRegister` interface for platform-specific implementations
- [x] Add factory functions (`getOrCreateRustRegister`, `getRustRegister`, etc.)

### Task 7.2: Create feature flag for Rust Register ✅
**References**: Design - Migration Strategy

- [x] Add `FEATURE_FLAGS.USE_RUST_REGISTER` to `config.ts`
- [x] Add `setUseRustRegister()` and `isRustRegisterEnabled()` functions
- [x] Export feature flag from `exports.ts` via `cojsonInternals`

### Task 7.3: Update `LocalNode` to use Register ✅
**References**: US-1

- [x] Import `RustRegister` type in `LocalNode`
- [x] Add optional `register?: RustRegister` property
- [x] Add `setRegister()` method
- [x] Update `internalDeleteCoValue()` to also free from Register

### Task 7.4: Update `VerifiedState` to use Register for headers ✅
**References**: US-2, Design - Phase 2

- [x] Add `dualWriteHeaderToRegister()` method in `CoValueCore`
- [x] Call dual-write from `provideHeader()` after creating VerifiedState
- [x] Also create session map in Register when header is stored
- [ ] Add integration tests comparing both sources (future task)

### Task 7.5: Update `SessionMap` to use Register ✅
**References**: US-3, Design - Phase 3

- [x] Add `dualWriteTransactionsToRegister()` method in `CoValueCore`
- [x] Call dual-write from `tryAddTransactions()` after VerifiedState update
- [ ] Add integration tests comparing outputs (future task)

### Task 7.6: Update `newContentSince` to use Rust implementation ✅
**References**: Design - Phase 4

- [x] Add `newContentSinceFromRust()` private method in `CoValueCore`
- [x] Add `convertRustContentMessage()` and `convertRawTransactionData()` helpers
- [x] Modify `newContentSince()` to use Rust when feature flag enabled
- [x] Fall back to TypeScript implementation on errors
- [ ] Add tests comparing Rust vs TypeScript outputs (future task)

---

## Phase 8: Benchmarks & Validation

### Task 8.1: Create baseline benchmarks
**References**: US-10, NFR-1

- [ ] Create `bench/cojson/register.bench.ts`
- [ ] Add benchmark: `newContentSince()` with N transactions (N = 10, 100, 1000)
- [ ] Add benchmark: Transaction iteration throughput
- [ ] Add benchmark: `getCurrentContent()` latency
- [ ] Record baseline results with TypeScript implementation

### Task 8.2: Add Rust benchmarks
**References**: US-10

- [ ] Add Rust benchmarks in `crates/cojson-core/benches/`
- [ ] Benchmark `get_content_since` with various transaction counts
- [ ] Benchmark transaction storage operations

### Task 8.3: Compare TypeScript vs Rust performance
**References**: US-10, NFR-1

- [ ] Run both implementations with identical data
- [ ] Verify no more than 5% regression
- [ ] Document improvements
- [ ] Add CI benchmark gate

### Task 8.4: Integration tests for dual-path comparison
**References**: Design - Regression Prevention

- [ ] Create tests that run both paths and compare outputs
- [ ] Test `newContentSince` with various scenarios
- [ ] Test `getValidTransactions` equivalence
- [ ] Test `getKnownState` equivalence

---

## Phase 9: Cleanup & Migration Complete

### Task 9.1: Switch reads to Rust Register
**References**: Design - Phase 2 step 6

- [ ] Update `VerifiedState` to read headers from Rust
- [ ] Update `SessionMap` to read from Rust
- [ ] Verify all tests pass

### Task 9.2: Remove TypeScript `newContentSince` implementation
**References**: Design - Phase 4 step 6

- [ ] Remove TypeScript implementation after benchmarks confirm parity
- [ ] Remove feature flag
- [ ] Update tests to only use Rust path

### Task 9.3: Remove dual-write code
**References**: Design - Phase 6

- [ ] Remove TypeScript header storage from `VerifiedState`
- [ ] Remove TypeScript transaction storage from `SessionMap`
- [ ] Clean up any remaining dual-write logic

### Task 9.4: Final cleanup
**References**: Design - Phase 6

- [ ] Remove unused TypeScript types/interfaces
- [ ] Remove feature flags
- [ ] Update code comments and inline documentation
- [ ] Run full test suite and benchmarks

---

## Notes

### What Does NOT Move to Rust (Intentionally)

These stay in TypeScript due to cross-CoValue dependencies:
- `determineValidTransactions()` - needs group state
- `decryptTransactionChangesAndMeta()` orchestration - needs key from group
- `RawGroup.roleOfInternal()` - recursive parent traversal  
- `RawGroup.getReadKey()` - cross-CoValue key lookup
- `atTime()` filtering - complex time-travel views

### Testing Strategy

- Each Rust module has unit tests
- Each binding (NAPI/WASM/RN) has integration tests
- TypeScript wrapper has unit tests
- Dual-path comparison tests during migration
- Benchmarks run on every PR

### Rollback Strategy

- Feature flags allow instant rollback
- Dual-write enables gradual migration
- Keep TypeScript implementation functional until Phase 9
