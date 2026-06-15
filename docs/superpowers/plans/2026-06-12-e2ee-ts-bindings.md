# E2EE TS Bindings & Typed App Implementation Plan (Plan 4 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface plan 3's runtime E2EE in TypeScript: WASM/NAPI binding methods, `db.e2ee.publicKey()`, `db.shareKey` / `db.unshareKey` / `db.keyHolders` gated to encryption-space tables at the type level, the `Locked` sentinel with `T | Locked` read typing, and the end-to-end black-box test catalogue from spec §10.

**Architecture:** The bindings are thin: every operation already exists on the Rust runtime (plan 3); TS adds typed wrappers and the `Locked` value mapping. The typed-app layer brands encryption-space tables (`DefinedTable.isEncryptionSpace` from plan 2) so key methods are compile-time gated, and brands encrypted columns so `RowOf` read types become `T | Locked` while insert/update input types stay `T`. Spec: `docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md` (§4–§6).

**Tech Stack:** Rust (`crates/jazz-wasm`, `crates/jazz-napi` — thin forwarding only), TS (`packages/jazz-tools/src`).

**Branch:** continue on `guido/e2ee-crypto-core`.

**Known anchors (verified in source):**

- WASM runtime class: `crates/jazz-wasm/src/runtime.rs` (`WasmRuntime::new` ~line 1375, static `mintJazzSelfSignedToken` ~line 1980, `connect(url, auth_json)` ~line 2011, `update_auth` ~line 2052).
- TS runtime: `packages/jazz-tools/src/runtime/wasm-runtime-module.ts` (`mintWasmToken` shows the secret string already reaches the TS runtime layer); `packages/jazz-tools/src/runtime/db.ts` `class Db` ~line 804 with `db.insert(app.todos, {...})` / `db.update(app.todos, id, {...})` style — **key methods follow this style**: `db.shareKey(app.projects, ...)`, not `db.projects.shareKey(...)` (the spec sketch predates the actual Db shape; record the deviation in the spec, Step 6.3).
- Typed app: `packages/jazz-tools/src/typed-app.ts` (`DefinedTable` with `isEncryptionSpace` from plan 2; `createAppForTables` builds the `app.<table>` handles ~line 1313).
- DSL branding precedent: `TypedColumnBuilder`'s phantom fields `__jazzValue` etc. in `packages/jazz-tools/src/dsl.ts:101`.
- Wire value mapping: TS `Value` type in `packages/jazz-tools/src/drivers/types.ts`; Rust emits `{"type":"Locked"}` (plan 3 Task 5) in human-JSON and a binary tag in native row deltas.

**Conventions:** black-box tests through the public API; integration tests live in `packages/jazz-tools/tests/ts-dsl/` (real WASM runtime); no AI attribution in commits; commands run from repo root unless noted.

---

### Task 1: Binding surface (WASM + NAPI)

**Files:**

- Modify: `crates/jazz-wasm/src/runtime.rs`
- Modify: `crates/jazz-napi/src/lib.rs` (mirror; find the parallel runtime impl via `grep -n "fn insert\|fn update_auth" crates/jazz-napi/src/lib.rs | head`)
- Test: existing binding smoke-test location (discovery: `grep -rn "mintJazzSelfSignedToken" crates/jazz-wasm/src/ packages/jazz-tools/src/ --include="*.test.ts" | head`)

- [ ] **Step 1.1: Static key derivation**

Next to `mintJazzSelfSignedToken` (same secret-string decoding it already uses — find the base64url seed parsing in that fn and reuse it):

```rust
    /// Derive the E2EE public key (base64url) from a LocalFirst Auth secret.
    #[wasm_bindgen(js_name = "deriveE2eePublicKey")]
    pub fn derive_e2ee_public_key_static(secret: String) -> Result<String, JsValue> {
        let seed = decode_auth_secret(&secret)?; // same helper minting uses
        Ok(jazz_tools::e2ee::derive_e2ee_keypair(&seed).public.to_base64url())
    }
```

- [ ] **Step 1.2: Instance methods**

Forwarding methods on `WasmRuntime` (and the NAPI runtime), all delegating to the plan-3 runtime/client methods:

```rust
    #[wasm_bindgen(js_name = "enableE2ee")]
    pub fn enable_e2ee(&self, secret: String) -> Result<(), JsValue>;

    #[wasm_bindgen(js_name = "e2eePublicKey")]
    pub fn e2ee_public_key(&self) -> Option<String>;

    #[wasm_bindgen(js_name = "shareKey")]
    pub fn share_key(&self, space_table: String, space_id: String,
        recipient_public_key: String, recipient_user_id: String) -> Result<String, JsValue>; // BatchId

    #[wasm_bindgen(js_name = "unshareKey")]
    pub fn unshare_key(&self, space_table: String, space_id: String,
        recipient_public_key: String) -> Result<String, JsValue>;

    #[wasm_bindgen(js_name = "keyHolders")]
    pub fn key_holders(&self, space_table: String, space_id: String) -> js_sys::Promise; // [{user_id, public_key}]
```

Error mapping: `E2eeKeyUnavailable` must surface with a stable, matchable message prefix (`"E2EE key unavailable"`) — the TS layer re-wraps it (Task 3.3). Uuid params parse from string with clear errors. Follow the file's existing argument/Promise conventions (look at how `query` returns promises in the same impl).

- [ ] **Step 1.3: Auto-enable on LoFi auth**

Where `connect`/`update_auth` parse `auth_json` and mint the LoFi token from the secret, also call `enable_e2ee` with the same secret (and clear E2EE state when auth switches away from LoFi). This gives the spec's "LoFi users automatically have their key available" with zero TS wiring.

- [ ] **Step 1.4: Build + commit**

Run: `cargo check -p jazz-wasm -p jazz-napi` (native check; full wasm-pack build happens in Task 5).

```bash
git add crates/jazz-wasm crates/jazz-napi
git commit -m "feat(jazz-wasm,jazz-napi): expose e2ee runtime methods"
```

---

### Task 2: `Locked` sentinel in TS

**Files:**

- Create: `packages/jazz-tools/src/locked.ts`
- Modify: `packages/jazz-tools/src/drivers/types.ts` (wire `Value` union)
- Modify: the wire-value→JS decoding path (discovery: `grep -rn "ValueHuman\|\"type\":\|fromWireValue\|decodeValue" packages/jazz-tools/src/runtime/ packages/jazz-tools/src/drivers/ | grep -v test | head`)
- Modify: `packages/jazz-tools/src/index.ts` (export)
- Test: `packages/jazz-tools/src/locked.test.ts`

- [ ] **Step 2.1: Sentinel**

```ts
// packages/jazz-tools/src/locked.ts
/**
 * An encrypted value whose space key is not available on this client
 * (not shared with this user, or key rows not yet synced).
 */
const LOCKED_BRAND: unique symbol = Symbol.for("jazz.e2ee.locked");

export interface Locked {
  readonly [LOCKED_BRAND]: true;
}

export const Locked: Locked = Object.freeze({ [LOCKED_BRAND]: true } as Locked);

export function isLocked(value: unknown): value is Locked {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<symbol, unknown>)[LOCKED_BRAND] === true
  );
}
```

`Symbol.for` (not a private symbol) so duplicated module instances in odd bundler setups still agree.

- [ ] **Step 2.2: Wire mapping**

Add `| { type: "Locked" }` to the wire `Value`/row-value union in `drivers/types.ts` and map it to the `Locked` sentinel at the single point wire values become JS values (found in discovery). Native row deltas: extend the binary decoder with the tag chosen in plan 3 Task 5.1. Writes must never serialize `Locked` — the value encoder throws `"Locked values cannot be written"` if it sees one.

- [ ] **Step 2.3: Tests + commit**

Unit tests: `isLocked(Locked)`, `isLocked({})` false, decode of `{type:"Locked"}` → sentinel, encode of sentinel throws.

```bash
git add packages/jazz-tools/src/locked.ts packages/jazz-tools/src/locked.test.ts packages/jazz-tools/src/drivers/ packages/jazz-tools/src/runtime/ packages/jazz-tools/src/index.ts
git commit -m "feat(jazz-tools): Locked sentinel for unavailable e2ee values"
```

---

### Task 3: `db` API + typed gating

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts` (key methods + `e2ee` namespace)
- Modify: `packages/jazz-tools/src/typed-app.ts` (space-table branding)
- Modify: `packages/jazz-tools/src/dsl.ts` (encrypted-column read-type branding)
- Test: `packages/jazz-tools/src/e2ee-db.test.ts` (type-level + unit), E2E in Task 4

- [ ] **Step 3.1: Space-table branding**

`createAppForTables` table handles gain `readonly __jazzEncryptionSpace?: true` when the source `DefinedTable.isEncryptionSpace` is set (plumb alongside how the handle already carries the table name; discovery: `grep -n "createAppForTables" -A 30 packages/jazz-tools/src/typed-app.ts`). Export a helper type:

```ts
export type EncryptionSpaceTable = { readonly __jazzEncryptionSpace: true };
```

- [ ] **Step 3.2: Encrypted-column read typing**

In `dsl.ts`, `encrypted(spaceRef)` (from plan 2) additionally flips a phantom: `__jazzEncrypted: true`, and the read-side row type maps branded columns to `Value | Locked`. Discovery for the mapping point: `grep -n "RowOf\|__jazzValue" packages/jazz-tools/src/typed-app.ts packages/jazz-tools/src/dsl.ts | head -15` — `RowOf` derives from `__jazzValue`; introduce `__jazzReadValue` (defaults to `__jazzValue`; `encrypted()` sets it to `Value | Locked`) and use it in the row-output type only, leaving insert/update input types on `__jazzValue`. Verify with type-level tests (`expectTypeOf` in vitest, matching existing type tests — `grep -rn "expectTypeOf" packages/jazz-tools/src | head -3` for the convention).

- [ ] **Step 3.3: Db methods**

On `class Db` (db.ts:804), following the `db.insert(app.todos, ...)` call style and its table-handle plumbing:

```ts
  /** This client's E2EE public key (base64url), or null when E2EE is off. */
  e2eePublicKey(): string | null;

  /** Seal the space key for a recipient and persist their sealed copy. */
  shareKey<T extends EncryptionSpaceTable>(table: T, spaceId: string,
    recipient: { publicKey: string; userId: string }): Promise<void>;

  /** Remove a recipient's sealed copy (v1 revocation is policy-only). */
  unshareKey<T extends EncryptionSpaceTable>(table: T, spaceId: string,
    recipientPublicKey: string): Promise<void>;

  /** Sealed-copy holders for a space row. */
  keyHolders<T extends EncryptionSpaceTable>(table: T, spaceId: string):
    Promise<{ userId: string; publicKey: string }[]>;
```

Each forwards to the binding methods from Task 1 with the table name off the handle. Runtime errors with the `"E2EE key unavailable"` prefix re-throw as a typed `E2eeKeyUnavailableError` (new error class exported from `locked.ts`'s module or a sibling `e2ee-errors.ts`). Calling with a non-space table is a TS compile error via the generic bound _and_ a runtime error from Rust (defense in depth).

- [ ] **Step 3.4: Tests + commit**

Unit/type tests: compile-time rejection of `db.shareKey(app.todos, ...)` for a non-space table (`// @ts-expect-error`); `RowOf` of an encrypted column is `string | Locked` while insert input stays `string`.

```bash
git add packages/jazz-tools/src
git commit -m "feat(jazz-tools): typed db e2ee key management API"
```

---

### Task 4: End-to-end black-box tests (spec §10)

**Files:**

- Create: `packages/jazz-tools/tests/ts-dsl/e2ee.test.ts`
- Modify (fixture): `packages/jazz-tools/tests/ts-dsl/fixtures/` — add an e2ee schema fixture following `fixtures/basic/schema.ts` (see how PR #1017 added fixture columns)

- [ ] **Step 4.1: Discovery — two-client harness**

The ts-dsl tests boot real runtimes. Find how `query-api.test.ts` constructs its app/db and whether a second client against the same server/storage is supported: `grep -n "createDb\|connect\|beforeAll" packages/jazz-tools/tests/ts-dsl/query-api.test.ts | head -10`. If the harness is single-client, the sharing scenarios run through the Rust integration tests (plan 3 Task 6.3) and this file covers the single-client scenarios plus `Locked` via a second runtime with a different secret against the same server — mirror whatever `cross-device`/sync tests already do (`grep -rln "two clients\|second client" packages/jazz-tools/tests/ | head`).

- [ ] **Step 4.2: Scenarios**

Implement the spec §10 catalogue from the TS surface (numbering from the spec):

1. round-trip (insert encrypted → read plaintext, `todo.title` is a plain string)
2. server blindness — fetch the raw row via a backend/admin context or raw query that bypasses decryption (whatever the harness exposes; the Rust twin already pins this, here it guards the TS plumbing) and assert ciphertext
3. share → second client reads plaintext
4. locked state → `isLocked(row.title)` true for an unshared client that can read the row by policy
5. unshare → `keyHolders` shrinks
6. write-without-key → rejects with `E2eeKeyUnavailableError`
7. concurrent invites from two clients → both sealed rows survive
8. restart persistence → recreate db with same secret + storage, plaintext readable without re-share
9. `$keys` update rejected by policy

(9 context-binding and 10 Rust-parity live in the Rust suite.)

- [ ] **Step 4.3: Run + commit**

Run: `cd packages/jazz-tools && pnpm exec vitest run tests/ts-dsl/e2ee.test.ts --config vitest.config.ts` (requires the rebuilt WASM artifact — Task 5 ordering note below; build first if the methods are missing).

```bash
git add packages/jazz-tools/tests
git commit -m "test(jazz-tools): e2ee end-to-end scenarios"
```

---

### Task 5: WASM rebuild + full verification

- [ ] **Step 5.1:** Rebuild the WASM artifact so TS tests exercise the new runtime: `cd crates/jazz-wasm && pnpm build` (wasm-pack `--release`). Note: Task 4 tests cannot pass before this; if executing tasks in order, run this build between Tasks 1 and 4 as well.
- [ ] **Step 5.2:** `cargo test -p jazz-tools` and `cd packages/jazz-tools && pnpm test` — all green except failures reproducible on `main` (the stale-WASM empty-`in` failure should _disappear_ after the rebuild; if it persists, it is a real bug to surface, not to fix here).
- [ ] **Step 5.3:** Measure the **bundle delta** promised by spec §9: compare `jazz_wasm_bg.wasm` size (raw + `gzip -9`) against the pre-E2EE artifact (`git stash` the Cargo changes or rebuild from `origin/main` in a worktree). Record both numbers in spec §9, replacing the "must be measured during implementation" caveat.
- [ ] **Step 5.4:**

```bash
git add docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md
git commit -m "docs: record measured e2ee bundle delta"
```

---

### Task 6: Spec/docs alignment

- [ ] **Step 6.1:** Update spec §4 code examples to the real API shape (`db.shareKey(app.projects, id, {publicKey, userId})` instead of `db.projects.shareKey(...)`; `db.e2eePublicKey()` instead of `db.e2ee.publicKey()`), with a one-line note that the Db surface is handle-parameterized rather than per-table namespaced. Also align the §5 Rust sketch: `share_key` takes `recipient_user_id` too (the `$keys` row records it for member listing).
- [ ] **Step 6.2:** Move `specs/todo/b_launch/e2ee_per_column.md`'s content pointer: the file was deleted from the working tree earlier — confirm with the user whether to commit that deletion alongside this plan's docs or restore it with a "superseded by" pointer. **Do not resolve unilaterally.**
- [ ] **Step 6.3:**

```bash
git add docs
git commit -m "docs: align e2ee spec examples with implemented API"
```

---

## Completion

After Task 6 the spec's v1 scope is fully implemented: identity (plan 1), schema (plan 2), runtime (plan 3), surface + E2E (plan 4). Remaining known follow-ups (tracked in spec §11, not planned): key epochs/rotation, `created_by` policy tightening, PQ hybrid sealing, `ExistsRel` encrypted-column validation, community LoFi-auth documentation.
