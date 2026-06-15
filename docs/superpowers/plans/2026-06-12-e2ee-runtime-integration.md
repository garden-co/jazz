# E2EE Runtime Integration Implementation Plan (Plan 3 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make E2EE live in the Rust runtime: an in-memory key service derived from the auth seed, atomic space-key bootstrap on space-row insert, transparent encrypt on write and decrypt on read with a `Value::Locked` fallback, `share_key` / `unshare_key` / `key_holders` on `JazzClient`, and `validate_e2ee_schema` enforced at schema ingestion.

**Architecture:** A new `E2eeService` owned by the runtime core composes plan 1's pure crypto (`crates/jazz-tools/src/e2ee.rs`) with plan 2's schema markers (`encrypted_with`, `encryption_space`, `$keys` tables). Writes are intercepted in `runtime_core/writes.rs` _before_ `schema_manager` (plaintext never reaches storage); reads are decrypted where result rows materialize. The sealed-key rows themselves are ordinary rows in `<table>$keys`, written through the normal write path so sync/policies apply unchanged. Spec: `docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md` (§4–§7).

**Tech Stack:** Rust only (`crates/jazz-tools`). Bindings and TS surface are plan 4.

**Branch:** continue on `guido/e2ee-crypto-core`.

**Known anchors (verified in source):**

- Write path: `runtime_core/writes.rs:563` `insert` → `insert_with_id` → `self.schema_manager.insert(...)`; `update` at `:605` resolves the table _inside_ `schema_manager` (`load_row_for_schema_update_in_context`). `begin_batch(batch_mode)` exists at `writes.rs:12`; multi-row batches are exercised by `runtime_core/tests/write_batch/`.
- `Value` enum: `query_manager/types/value.rs:15` (no `Locked` yet); human-JSON serde via internally tagged `ValueHuman` in the same file.
- Crypto: `e2ee::{derive_e2ee_keypair, SpaceKey, seal_space_key, unseal_space_key, encrypt_value, decrypt_value, envelope_key_id, EncryptionContext, E2eeError}`.
- Schema helpers: `query_manager::types::e2ee_schema::{e2ee_keys_table_name, validate_e2ee_schema}`; markers on `ColumnDescriptor.encrypted_with` / `TableSchema.encryption_space`.
- User id from seed: `identity::user_id_from_seed`-style helpers in `crates/jazz-tools/src/identity.rs` (see `user_id_from_public_key` near line 299; confirm the exact public fn when wiring).
- Rust client: `client.rs` `JazzClient::{connect(AppContext), insert, update, delete, query, schema}`.

**Plaintext serialization rule (normative):** an encrypted column's schema `column_type` stays the _logical_ type. Before encryption the plaintext `Value` is serialized with `postcard::to_allocvec` (already a dependency); after decryption it is restored with `postcard::from_bytes`. The stored/synced physical value is always `Value::Bytea(envelope)`.

**AAD rule (normative):** `EncryptionContext { table, column, row_id: object_id.as_bytes() }` — the row id is the 16-byte `ObjectId`, so inserts must allocate the `ObjectId` _before_ encrypting (pass `Some(id)` down the existing `insert_with_id` path).

**Conventions:** black-box integration tests in `crates/jazz-tools/tests/`; no AI attribution in commits; commands run from repo root.

---

### Task 1: `E2eeService` and runtime wiring

**Files:**

- Create: `crates/jazz-tools/src/runtime_core/e2ee_service.rs`
- Modify: `crates/jazz-tools/src/runtime_core/mod.rs` (runtime struct + module decl)
- Test: `crates/jazz-tools/tests/e2ee_runtime.rs` (new; grows through the plan)

- [ ] **Step 1.1: Discovery — runtime struct and module wiring**

Run: `grep -n "pub struct Runtime\|mod writes\|mod subscriptions" crates/jazz-tools/src/runtime_core/mod.rs | head`
Identify the runtime core struct (the type whose `impl` block contains `insert`/`update` in `writes.rs`) and where sibling modules are declared. The service field goes on that struct; the module declaration goes next to `mod writes;`.

- [ ] **Step 1.2: Implement the service**

Create `crates/jazz-tools/src/runtime_core/e2ee_service.rs`:

```rust
//! Runtime E2EE state: identity keypair, unsealed space-key cache.
//!
//! Pure crypto lives in `crate::e2ee`; this module owns the mutable state and
//! the "which key for which row" bookkeeping. Sealed keys at rest live in the
//! `<table>$keys` companion tables; only unsealed keys are cached here, in
//! memory, never persisted.

use std::collections::HashMap;

use uuid::Uuid;

use crate::e2ee::{derive_e2ee_keypair, E2eeKeypair, E2eePublicKey, SpaceKey};
use crate::object::ObjectId;

#[derive(Default)]
pub struct E2eeService {
    keypair: Option<E2eeKeypair>,
    /// space row id -> (key_id, unsealed key). v1: exactly one key per space.
    space_keys: HashMap<ObjectId, (Uuid, SpaceKey)>,
}

impl E2eeService {
    /// Enable E2EE for this runtime from the 32-byte LocalFirst Auth seed.
    pub fn enable(&mut self, seed: &[u8; 32]) {
        self.keypair = Some(derive_e2ee_keypair(seed));
    }

    pub fn is_enabled(&self) -> bool {
        self.keypair.is_some()
    }

    pub fn public_key(&self) -> Option<&E2eePublicKey> {
        self.keypair.as_ref().map(|kp| &kp.public)
    }

    pub fn keypair(&self) -> Option<&E2eeKeypair> {
        self.keypair.as_ref()
    }

    pub fn cached_space_key(&self, space_id: &ObjectId) -> Option<&(Uuid, SpaceKey)> {
        self.space_keys.get(space_id)
    }

    pub fn cache_space_key(&mut self, space_id: ObjectId, key_id: Uuid, key: SpaceKey) {
        self.space_keys.insert(space_id, (key_id, key));
    }

    /// Wipe all secrets (sign-out).
    pub fn clear(&mut self) {
        self.keypair = None;
        self.space_keys.clear();
    }
}
```

Add `mod e2ee_service;` (+ `pub use e2ee_service::E2eeService;` if the runtime's module style re-exports) and an `e2ee: E2eeService` field to the runtime struct, initialized `E2eeService::default()` at every construction site (compiler-guided: `cargo check -p jazz-tools`).

- [ ] **Step 1.3: New error variant**

Find the runtime error enum used by `writes.rs` (`grep -n "RuntimeError" crates/jazz-tools/src/runtime_core/mod.rs | head`) and add:

```rust
    /// An E2EE write or read needed a space key that is not available
    /// (not shared with this user, not yet synced, or E2EE not enabled).
    E2eeKeyUnavailable { table: String, space_id: String },
```

with a `Display` arm: `"E2EE key unavailable for table '{table}', space '{space_id}'"`. If `RuntimeError` lives in `query_manager` as `QueryError`, add the variant there instead — follow where `write_error_from_query` maps from.

- [ ] **Step 1.4: Compile + commit**

Run: `cargo check -p jazz-tools` → clean.

```bash
git add crates/jazz-tools/src/runtime_core/ crates/jazz-tools/src/query_manager/
git commit -m "feat(jazz-tools): runtime E2EE service skeleton"
```

---

### Task 2: Key lookup from `$keys` rows (cache fill)

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/e2ee_service.rs` + the runtime impl (new method)
- Test: `crates/jazz-tools/tests/e2ee_runtime.rs`

- [ ] **Step 2.1: Discovery — local row lookup**

The runtime must read local `<table>$keys` rows matching `space_id` without going through the public async query API. Find the internal lookup the write path already uses: `grep -n "load_row_for_schema_update_in_context\|load_latest_transactional_staged_row" crates/jazz-tools/src/schema_manager/manager.rs | head`. If there is no "rows by indexed column" internal helper, use the query manager's scan used by policy evaluation (`Exists` evaluation reads sibling tables — find it via `grep -rn "fn evaluate_exists\|scan_table" crates/jazz-tools/src/query_manager/ | head`). The deliverable of this step is one chosen call path, recorded as a comment in the implementation.

- [ ] **Step 2.2: Implement `space_key_for`**

On the runtime impl (next to the write methods so it can borrow `schema_manager` + `storage`):

```rust
    /// Return (key_id, space key) for a space row, unsealing from local
    /// `$keys` rows on first touch. Errors with E2eeKeyUnavailable when no
    /// sealed copy for this identity exists locally.
    pub(crate) fn space_key_for(
        &mut self,
        space_table: &str,
        space_id: ObjectId,
    ) -> Result<(Uuid, SpaceKey), RuntimeError> {
        if let Some((key_id, key)) = self.e2ee.cached_space_key(&space_id) {
            return Ok((*key_id, key.clone()));
        }
        let keypair = self.e2ee.keypair().ok_or_else(|| {
            RuntimeError::E2eeKeyUnavailable {
                table: space_table.to_string(),
                space_id: space_id.to_string(),
            }
        })?;
        // Scan local `<space_table>$keys` rows with space_id == space_id
        // (lookup path chosen in Step 2.1); for each row, try
        // e2ee::unseal_space_key(keypair, sealed_key). First success wins;
        // unseal failures are skipped (bogus rows, copies for other users).
        // On success: parse key_id (Uuid column), cache, return.
        // On no success: E2eeKeyUnavailable.
        todo_lookup_loop()
    }
```

Replace `todo_lookup_loop()` with the loop over the chosen lookup from Step 2.1 — the loop body above is normative (skip-on-unseal-failure, first success wins, cache on success).

- [ ] **Step 2.3: Tests**

Black-box, in `crates/jazz-tools/tests/e2ee_runtime.rs`, using the same in-process runtime harness existing write-path tests use (`grep -rn "fn test_runtime\|Runtime::new\|test_support" crates/jazz-tools/src/runtime_core/tests/write_batch/direct.rs | head` to find the constructor): insert `$keys` rows manually (plain inserts — schema from plan 2 makes them ordinary tables), then assert `space_key_for` unseals for the right identity, errors for a stranger, and skips a corrupted row before finding a good one. Commit:

```bash
git add crates/jazz-tools
git commit -m "feat(jazz-tools): unseal space keys from local \$keys rows"
```

---

### Task 3: Space-creation bootstrap (atomic key + creator's copy)

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/writes.rs` (`insert_with_id`)
- Test: `crates/jazz-tools/tests/e2ee_runtime.rs`

- [ ] **Step 3.1: Implement**

In `insert_with_id`, after `resolve_batch_write_context` and before delegating to `schema_manager.insert`: if the target table has `encryption_space == true` and `self.e2ee.is_enabled()`:

1. Ensure `object_id` is `Some` (generate `ObjectId::new()` if needed) — the `$keys` row needs it.
2. Generate `SpaceKey::generate()` + `key_id = Uuid::new_v4()`.
3. Seal to own public key: `seal_space_key(self.e2ee.public_key().unwrap(), &key)`.
4. Insert the space row (existing call), then insert the `$keys` row **with the same `write_context`/batch** via a recursive `self.insert_with_id(&e2ee_keys_table_name(table), keys_values, None, write_context)` where `keys_values` =
   `space_id: Value::Uuid(space_row_id)`, `key_id: Value::Uuid(ObjectId::from_uuid(key_id))`, `recipient_user_id: Value::Uuid(own user id)`, `recipient_public_key: Value::Text(pk.to_base64url())`, `sealed_key: Value::Bytea(sealed)`.
5. Cache the key (`cache_space_key`) before returning, so immediate encrypted inserts into the space work without a sync round-trip.

The "own user id" comes from the same seed via the identity module (Step 1.1 anchor); store it on `E2eeService` at `enable()` time as `ObjectId` to avoid re-deriving.

Atomicity: when the outer call is already inside a batch (`write_context.batch_id().is_some()`), the recursive insert shares it naturally. For direct writes, wrap: `begin_batch(BatchMode::Transactional)` → both inserts → `commit_batch` — match how `runtime_core/tests/write_batch/transactional.rs` drives multi-write batches. A space row must never commit without its `$keys` row.

If E2EE is _not_ enabled and the table is an encryption space, fail the insert with `E2eeKeyUnavailable` — a keyless space row violates the spec invariant (§3) and would strand other members.

- [ ] **Step 3.2: Tests + commit**

Tests: inserting a space row creates exactly one `$keys` row in the same batch (query both tables; check the `BatchId` equality if the harness exposes it); the creator can immediately encrypt into the space; insert into a space table without `enable()` errors.

```bash
git add crates/jazz-tools
git commit -m "feat(jazz-tools): atomic space key bootstrap on space-row insert"
```

---

### Task 4: Transparent encrypt on write

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/writes.rs` (`insert_with_id`, `update`, `upsert`)
- Modify: wherever insert values are type-checked against `ColumnType` (discovery below)
- Test: `crates/jazz-tools/tests/e2ee_runtime.rs`

- [ ] **Step 4.1: Discovery — physical-type exemption**

Encrypted columns carry their logical type in the schema but store `Value::Bytea`. Find every place that validates a written `Value` against `ColumnType`:
`grep -rn "fn align_insert_values\|type mismatch\|TypeMismatch" crates/jazz-tools/src/schema_manager/manager.rs crates/jazz-tools/src/query_manager/writes.rs | head -20`
At each site, accept `Value::Bytea` for a column whose descriptor has `encrypted_with.is_some()` (the value at that point is already an envelope — encryption happens before `schema_manager`). Add a focused comment at each exemption: `// E2EE: encrypted columns store envelope bytes; logical type applies to plaintext only.`

- [ ] **Step 4.2: Implement the encrypt hook**

Private helper on the runtime impl:

```rust
    /// Encrypt the values of encrypted columns in-place. `row_id` must be the
    /// final ObjectId of the row (AAD binds it).
    fn encrypt_values_for_write(
        &mut self,
        table: &str,
        row_id: ObjectId,
        values: &mut HashMap<String, Value>,
        space_ids: &HashMap<String, ObjectId>, // space_ref column -> space row id
    ) -> Result<(), RuntimeError> { /* per encrypted column present in `values`:
        - skip Value::Null (nullable encrypted columns store plain NULL);
        - resolve (key_id, key) = self.space_key_for(ref target table, space_ids[ref])?;
        - plaintext = postcard::to_allocvec(&value);
        - envelope = e2ee::encrypt_value(&key, &key_id, &EncryptionContext {
              table, column, row_id: row_id.as_bytes() }, &plaintext);
        - *value = Value::Bytea(envelope); */ }
```

Call sites:

- **insert/upsert:** table schema known up front. If any encrypted column is present: pre-generate `object_id` when `None`; read each needed `space_ref` value out of `values` (it is non-nullable and therefore required on insert; missing → existing missing-column error path); encrypt; proceed.
- **update:** the table isn't in the signature. Resolve it the way `upsert` does (`load_row_for_schema_update_in_context`); if the update touches encrypted columns, also read the space-ref value from the loaded existing row (the ref may legitimately be absent from the update). Then encrypt with `row_id = object_id`.

- [ ] **Step 4.3: Tests + commit**

Tests (black-box through the runtime): inserting into an encrypted column stores a `Value::Bytea` envelope (read the raw row through a runtime _without_ the key — see Task 5's Locked — or through storage inspection the way existing write tests assert stored rows); `envelope_key_id` of the stored bytes equals the space's `key_id`; update re-encrypts; write without key fails with `E2eeKeyUnavailable` and writes nothing.

```bash
git add crates/jazz-tools
git commit -m "feat(jazz-tools): transparent encryption of e2ee columns on write"
```

---

### Task 5: `Value::Locked` and transparent decrypt on read

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/types/value.rs` (`Value`, `ValueHuman`, postcard/row encodings)
- Modify: `crates/jazz-tools/src/row_format.rs` and/or wherever `Value` has a binary tag (discovery)
- Modify: result materialization point(s) in `runtime_core` (discovery)
- Test: `crates/jazz-tools/tests/e2ee_runtime.rs`

- [ ] **Step 5.1: Add the variant**

Add to `Value` (value.rs:15) and mirror in `ValueHuman`:

```rust
    /// An encrypted value whose space key is unavailable on this client.
    /// Result-only: never written to storage or accepted in writes.
    Locked,
```

Run `cargo check -p jazz-tools` and visit every non-exhaustive-match error. Rules for the arms:

- storage/row encodings: `Locked` is unreachable on the write side — `unreachable!("Value::Locked is result-only")` after the write hook rejects it (writes containing `Value::Locked` get a validation error at the same sites as Step 4.1);
- wire/result encodings (the JSON `ValueHuman`, subscription delta encoding): encode as a real tag (`{"type":"Locked"}` for JSON; the next free byte tag for the binary row encoding — find tags via `grep -n "TYPE_\|tag" crates/jazz-tools/src/row_format.rs | head -20`). Plan 4's TS layer maps it to the `Locked` sentinel.
- comparisons/indexing/policy evaluation: `Locked` never appears there (encrypted columns are excluded from indexes and policies by plan 2 validation); use `unreachable!` with the same message.

- [ ] **Step 5.2: Discovery — result materialization point**

Find where one-shot query results and subscription delta rows are produced as `Value`s in `runtime_core` (not per-binding):
`grep -n "fn query\|RowDelta\|emit" crates/jazz-tools/src/runtime_core/mod.rs crates/jazz-tools/src/runtime_core/subscriptions.rs | head -20`
The decrypt hook must run once, at the shared point both paths flow through, with access to the result schema's `ColumnDescriptor`s (needed to know which output columns carry `encrypted_with` — projections preserve the marker since plan 2's `project.rs` clones it).

- [ ] **Step 5.3: Implement decrypt**

```rust
    /// Decrypt encrypted columns in a result row in-place; key misses become
    /// Value::Locked, never errors (reads must not fail on missing keys).
    fn decrypt_row_values(&mut self, table: &str, row_id: ObjectId,
        descriptors: &[ColumnDescriptor], values: &mut [Value]) { /* per column
        with encrypted_with: if Value::Bytea(envelope):
        - space id comes from the sibling ref column's value in this row;
        - (key_id, key) = space_key_for(...); on E2eeKeyUnavailable -> Locked;
        - decrypt_value(&key, &EncryptionContext{..}, envelope); on Err -> Locked
          (tampered/foreign ciphertext must not crash reads; log at debug);
        - postcard::from_bytes -> restored Value. */ }
```

The space-ref column is part of the row by construction on full-row reads; for _projections that exclude the ref column_, fall back to `envelope_key_id` + a reverse map on the cache (`key_id -> SpaceKey`) — add that secondary index to `E2eeService` (`HashMap<Uuid, SpaceKey>` maintained alongside `space_keys`); if the key id isn't cached, the value is `Locked` (client can re-query with the ref included or after syncing keys).

- [ ] **Step 5.4: Tests + commit**

Tests: writer inserts encrypted row; reader runtime _with_ the key reads plaintext transparently; reader _without_ the key gets `Value::Locked` (and plaintext columns intact); corrupted envelope yields `Locked` not an error; ciphertext copied between rows via a raw write yields `Locked` (AAD context binding, spec §10 item 9); projection without the space-ref column still decrypts when the key is cached.

```bash
git add crates/jazz-tools
git commit -m "feat(jazz-tools): Value::Locked and transparent decryption on read"
```

---

### Task 6: `JazzClient` API + schema-ingestion validation

**Files:**

- Modify: `crates/jazz-tools/src/client.rs`
- Modify: runtime constructors that accept the schema (discovery) — call `validate_e2ee_schema`
- Test: `crates/jazz-tools/tests/e2ee_runtime.rs`

- [ ] **Step 6.1: Client methods**

On `JazzClient` (string-table-addressed, like `insert` at client.rs:288):

```rust
    /// This client's E2EE public key, if E2EE is enabled (LoFi auth seed present).
    pub fn e2ee_public_key(&self) -> Option<String>; // base64url

    /// Seal the space key for a recipient and insert the `$keys` row.
    /// Errors: table not an encryption space; key unavailable; bad recipient key.
    pub fn share_key(&self, space_table: &str, space_id: Uuid,
        recipient_public_key: &str, recipient_user_id: Uuid) -> Result<BatchId>;

    /// Delete the recipient's sealed copy (revocation is policy-only in v1).
    pub fn unshare_key(&self, space_table: &str, space_id: Uuid,
        recipient_public_key: &str) -> Result<BatchId>;

    /// List sealed-copy holders: (recipient_user_id, recipient_public_key).
    pub async fn key_holders(&self, space_table: &str, space_id: Uuid)
        -> Result<Vec<(Uuid, String)>>;
```

`share_key` = `space_key_for` + `seal_space_key` + plain insert into `e2ee_keys_table_name(space_table)`; `unshare_key` = query the row id by `(space_id, recipient_public_key)` + plain delete; `key_holders` = plain query. All three error with a clear message when the table lacks `encryption_space` in the schema. `recipient_user_id` is caller-supplied (the app knows it from its own directory row; the spec's `$keys` table records it for member listing).

E2EE enablement: thread the LoFi seed from `AppContext` (discovery: `grep -n "AppContext" crates/jazz-tools/src/client.rs | head` — wherever the self-signed-token seed already lives) into `runtime.e2ee.enable(seed)` at connect; `clear()` on disconnect/sign-out.

- [ ] **Step 6.2: Validate at ingestion**

Call `validate_e2ee_schema` wherever the runtime accepts a schema (runtime construction from a `Schema`, and schema updates from the catalogue): `grep -rn "fn new\|set_schema\|install_schema" crates/jazz-tools/src/runtime_core/mod.rs | head`. Reject invalid schemas with the validation error string.

- [ ] **Step 6.3: Tests + commit**

Tests: two in-process clients (writer/reader harness from Task 2.3): full share flow — A creates space, writes encrypted, `share_key`s to B's published key, B reads plaintext; `unshare_key` removes B's row (`key_holders` shrinks); non-space table errors; invalid schema rejected at runtime construction.

```bash
git add crates/jazz-tools
git commit -m "feat(jazz-tools): share_key/unshare_key/key_holders client API"
```

---

### Task 7: Spec §10 catalogue sweep (Rust side) + crate verification

- [ ] **Step 7.1:** Walk spec §10 items 1–11 and check each has a Rust-side test from Tasks 2–6 (items 3, 10 are Rust-native here; TS twins land in plan 4). Add any missing scenario to `tests/e2ee_runtime.rs` — notably **restart persistence** (drop the runtime, rebuild from the same storage + seed, decrypt without re-share) and **server blindness** (inspect stored row bytes for absence of plaintext).
- [ ] **Step 7.2:** `cargo test -p jazz-tools` all green; `cargo clippy -p jazz-tools` no new warnings.
- [ ] **Step 7.3:**

```bash
git add crates/jazz-tools
git commit -m "test(jazz-tools): e2ee runtime scenario coverage"
```

---

## Out of scope (plan 4)

- WASM/NAPI binding methods, `db.e2ee.publicKey()`, typed `shareKey`/`unshareKey`/`keyHolders`, TS `Locked` sentinel + `T | Locked` typing, TS E2E tests.
- Key epochs/rotation, `created_by` policy tightening, PQ hybrid (spec §11).
- Benchmark validating the cost-summary throughput estimate (add alongside Task 5 if trivial with an existing bench harness; otherwise file a follow-up).
