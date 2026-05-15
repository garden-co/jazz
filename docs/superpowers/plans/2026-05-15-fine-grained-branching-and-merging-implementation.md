# Fine-Grained Branching And Merging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. For larger slices, use superpowers:subagent-driven-development to split Rust runtime work, TypeScript API work, and integration tests across workers. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement sparse Jazz branches with write visibility isolation, inherited object-id permissions, query-builder branch selection, query-builder `.diff()`, and branch `.merge()` into `main`.

**Architecture:** Treat app branches as alternate user branches in the existing branch-aware row-history storage. Keep `main` as the implicit read fallback and merge/diff target. Add branch overlay reads to the query manager instead of using today's multi-branch LWW query behavior. Preserve row-history ancestry through cross-branch parent references so merges can parent to both source branch and `main`.

**Tech Stack:** Rust runtime and query manager, TypeScript `jazz-tools` API, WASM/NAPI bindings, existing row-history merge strategies, existing local policy evaluation.

---

## File Structure

- Modify: `packages/jazz-tools/src/runtime/db.ts`
  - Add branch-scoped `Db` view, branch-aware batch handles, `.merge()`, and diff execution plumbing.

- Modify: `packages/jazz-tools/src/runtime/client.ts`
  - Add branch-aware write context helpers and runtime calls for branch merge/diff.

- Modify: `packages/jazz-tools/src/typed-app.ts`
  - Add query-builder `.branch(branchId)` and `.diff()` types/build metadata.

- Modify: `packages/jazz-tools/src/runtime/query-builder-shape.ts`
  - Normalize branch and diff metadata from generated and generic builders.

- Modify: `packages/jazz-tools/src/runtime/query-adapter.ts`
  - Pass branch and diff mode into the runtime query JSON.

- Modify: `packages/jazz-tools/src/magic-columns.ts`
  - Add `$diff` as a reserved/magic output column.

- Modify: `crates/jazz-tools/src/query_manager/query.rs`
  - Extend `Query` with branch overlay and diff mode metadata.

- Modify: `crates/jazz-tools/src/query_manager/manager.rs`
  - Implement branch overlay reads, query-scoped diff, and branch permission checks.

- Modify: `crates/jazz-tools/src/query_manager/writes.rs`
  - Add explicit branch write helpers and branch-aware first-write parent selection.

- Modify: `crates/jazz-tools/src/row_histories/*`
  - Add cross-branch parent refs and reuse merge-strategy resolution for merge preview.

- Modify: `crates/jazz-tools/src/runtime_core/*`
  - Add runtime operations for branch diff and merge.

- Modify: `crates/jazz-tools/src/query_manager/bindings.rs`
  - Expose branch/diff/merge runtime bindings to WASM.

- Modify: `packages/jazz-tools/src/types/jazz-wasm.d.ts`
  - Add the generated WASM surface types.

- Add tests under:
  - `crates/jazz-tools/src/query_manager/manager_tests/branches.rs`
  - `crates/jazz-tools/src/runtime_core/tests/`
  - `packages/jazz-tools/src/runtime/*.test.ts`

---

### Task 1: Add Branch Addressing Helpers

**Files:**

- Modify: `packages/jazz-tools/src/runtime/client.ts`
- Modify: `crates/jazz-tools/src/query_manager/types/branch.rs`
- Modify: `crates/jazz-tools/src/schema_manager/manager.rs`

- [ ] **Step 1: Define public branch id validation**

Accept only:

- `"main"` for the system branch
- Jazz object id strings for app branches

Reject empty strings and non-UUID app branch ids at the TypeScript API boundary where possible, and again in Rust for enforcing runtimes.

- [ ] **Step 2: Compose storage branch names from object ids**

Add a helper equivalent to the current schema branch composition:

```text
{env}-{schema_hash_short}-{user_branch}
```

where `user_branch` is `main` or the object id string passed to `db.branch(objectId)`.

- [ ] **Step 3: Keep schema-version branch behavior intact**

Do not remove the existing schema hash branch mechanism. App branch ids become the `user_branch` segment; schema migrations still change the hash segment.

- [ ] **Step 4: Test composition and parsing**

Add tests that:

- `main` maps to the current composed main branch
- a UUID branch id maps to the same env/schema hash with the UUID as user branch
- invalid app branch ids are rejected

---

### Task 2: Add Branch-Scoped TypeScript API

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`
- Modify: `packages/jazz-tools/src/runtime/client.ts`
- Modify: `packages/jazz-tools/src/runtime/db.persisted.test.ts`

- [ ] **Step 1: Add `Db.branch(branchId)`**

Return a lightweight branch-scoped database view that carries:

- the root `Db`
- the public branch id
- the composed storage branch name

The view must not construct a new runtime or storage instance.

- [ ] **Step 2: Route writes through branch context**

Make these methods write to the branch storage name:

```ts
db.branch(branch.id).insert(app.todos, values);
db.branch(branch.id).update(app.todos, todo.id, patch);
db.branch(branch.id).delete(app.todos, todo.id);
```

Use the existing write context payload rather than adding separate runtime methods for each CRUD operation.

- [ ] **Step 3: Add branch-scoped reads**

Make `db.branch(branch.id).all(query)` and `.one(query)` use the branch as the default query branch unless the query itself selects a branch.

- [ ] **Step 4: Preserve batch and transaction ergonomics**

Decide whether `db.branch(id).batch(...)` and `db.branch(id).transaction(...)` are included in the first implementation. If included, the batch context must use the branch storage name as its target branch.

- [ ] **Step 5: Add API tests**

Cover:

- branch view writes do not affect root `db` writes
- query-level branch selection overrides branch-scoped db default
- branch-scoped batches, if included, write only to the selected branch

---

### Task 3: Implement Branch Permission Gates

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/manager.rs`
- Modify: `crates/jazz-tools/src/query_manager/server_queries.rs`
- Modify: `crates/jazz-tools/src/query_manager/writes.rs`
- Modify: `crates/jazz-tools/src/query_manager/rebac_tests/*.rs`

- [ ] **Step 1: Resolve backing object**

For every non-main branch id, resolve the object id to its row locator and visible backing row.

- [ ] **Step 2: Gate branch read operations**

Require read permission on the backing object for:

- `db.branch(id).all(...)`
- query-builder `.branch(id)`
- query-builder `.diff()`

- [ ] **Step 3: Gate branch write operations**

Require update permission on the backing object for:

- branch insert
- branch update
- branch delete
- branch merge

- [ ] **Step 4: Preserve data permissions**

After the branch-access gate passes, still run normal row/table policies for the data being read or written. Branch access must not grant access to unrelated rows.

- [ ] **Step 5: Add permissive-runtime fallback**

In local runtimes without a loaded policy bundle, keep current permissive behavior. Enforcing runtimes must fail closed if the backing object cannot be resolved or read/updated.

- [ ] **Step 6: Add permission tests**

Cover:

- owner can read/write/diff/merge their branch
- non-owner cannot read the branch
- non-owner cannot write through the branch
- branch access does not bypass todo/comment row policies

---

### Task 4: Add Branch Overlay Read Semantics

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/query.rs`
- Modify: `crates/jazz-tools/src/query_manager/graph/compile.rs`
- Modify: `crates/jazz-tools/src/query_manager/graph_nodes/index_scan.rs`
- Modify: `crates/jazz-tools/src/query_manager/manager.rs`
- Modify: `crates/jazz-tools/src/storage/storage_trait.rs`

- [ ] **Step 1: Add query metadata**

Extend runtime `Query` with:

```rust
pub branch_overlay: Option<String>
pub diff_mode: bool
```

Keep existing `branches` for schema/version fanout and internal multi-branch scans.

- [ ] **Step 2: Implement direct row overlay loading**

For a selected branch:

```text
if branch has visible row or tombstone:
  use branch row
else:
  use main row
```

Soft/hard branch tombstones must hide the `main` row.

- [ ] **Step 3: Implement query candidate collection**

For branch overlay queries, candidate row ids are:

- rows matching indexes on `main`
- rows matching indexes on the selected branch
- rows overridden or deleted by the selected branch when needed to apply post-overlay filters

The final filter must be evaluated against the overlay row, not against the raw `main` row.

- [ ] **Step 4: Keep schema lenses working**

When a row comes from a branch with a different schema hash, use the existing lens path before evaluating filters and returning rows.

- [ ] **Step 5: Update subscriptions**

Branch overlay subscriptions must depend on both:

- source branch writes
- `main` writes that affect fallback rows

- [ ] **Step 6: Add overlay tests**

Cover:

- branch read falls back to `main`
- branch update overrides `main`
- branch delete hides `main`
- branch insert appears only on branch
- branch query includes rows moved into the filter by branch edits
- branch query excludes rows moved out of the filter by branch edits

---

### Task 5: Add Branch-Aware Parent Links

**Files:**

- Modify: `crates/jazz-tools/src/row_histories/types.rs`
- Modify: `crates/jazz-tools/src/row_histories/codecs.rs`
- Modify: `crates/jazz-tools/src/row_histories/resolution.rs`
- Modify: `crates/jazz-tools/src/row_histories/mutations.rs`
- Modify: `crates/jazz-tools/src/storage/*`

- [ ] **Step 1: Introduce `ParentRef`**

Add an internal parent type:

```rust
struct ParentRef {
    branch: BranchName,
    batch_id: BatchId,
}
```

- [ ] **Step 2: Preserve backward compatibility**

Decode old `_jazz_parents` batch-id arrays as same-branch parent refs.

- [ ] **Step 3: Encode cross-branch parents**

For new writes, encode parent refs so branch and batch id are both available. Keep the encoding compact for same-branch parents.

- [ ] **Step 4: Update ancestry traversal**

MRCA and visible resolution must resolve parent refs by `(row_id, branch, batch_id)`, not by bare batch id.

- [ ] **Step 5: Add cross-branch parent tests**

Cover:

- old rows decode correctly
- same-branch histories behave unchanged
- cross-branch parent refs resolve
- unresolved cross-branch parents produce a clear error

---

### Task 6: Fix Branch Write Parenting

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/writes.rs`
- Modify: `crates/jazz-tools/src/schema_manager/manager.rs`
- Modify: `crates/jazz-tools/src/runtime_core/writes.rs`

- [ ] **Step 1: Parent later branch writes to branch tips**

If the row already has a branch tip, use that branch frontier as parents.

- [ ] **Step 2: Parent first branch write to visible `main`**

If the selected branch has no row history for the row, load the visible `main` frontier and use those parent refs.

- [ ] **Step 3: Keep branch inserts parentless**

If the row does not exist on `main`, branch insert writes no `main` parent.

- [ ] **Step 4: Keep branch tombstones parented**

Branch deletes of `main` rows should parent to `main` on first delete and to the branch tip on later deletes.

- [ ] **Step 5: Add write-parent tests**

Cover first branch update, second branch update, branch insert, and branch delete.

---

### Task 7: Add Query Builder `.branch(...)` And `.diff()`

**Files:**

- Modify: `packages/jazz-tools/src/typed-app.ts`
- Modify: `packages/jazz-tools/src/runtime/query-builder-shape.ts`
- Modify: `packages/inspector/src/utility/generic-query-builder.ts`
- Modify: `packages/jazz-tools/src/runtime/query-adapter.ts`

- [ ] **Step 1: Add `.branch(branchId)`**

Add a chainable method to typed and generic builders. Store the public branch id in built query JSON.

- [ ] **Step 2: Add `.diff()`**

Add a chainable method that marks the built query as diff mode. It takes no arguments.

- [ ] **Step 3: Normalize branch/diff metadata**

Ensure `normalizeBuiltQuery(...)` preserves branch id and diff mode for runtime translation.

- [ ] **Step 4: Update types**

Add `QueryDiffRow<Row>` with a `$diff` magic column:

```ts
type QueryDiffRow<Row> = Row & {
  $diff: {
    kind: "insert" | "update" | "delete" | "unchanged" | "error";
    changed: string[];
    conflicts: string[];
    error?: {
      code:
        | "unresolved_parent"
        | "missing_common_ancestor"
        | "schema_error"
        | "merge_strategy_error";
      message: string;
    };
  };
};
```

- [ ] **Step 5: Add TypeScript API tests**

Cover generated builder JSON, generic builder JSON, `.diff()` return type, and branch override behavior.

---

### Task 8: Implement Query-Scoped Diff

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/manager.rs`
- Modify: `crates/jazz-tools/src/query_manager/magic_columns.rs`
- Modify: `crates/jazz-tools/src/query_manager/graph_nodes/magic_columns.rs`
- Modify: `packages/jazz-tools/src/magic-columns.ts`

- [ ] **Step 1: Add `$diff` magic column**

Reserve `$diff` and ensure user schemas cannot define it.

- [ ] **Step 2: Compute candidate scope**

For `.branch(source).where(...).diff()`:

1. Evaluate the query against the source branch overlay.
2. Evaluate the same query against `main`.
3. Union the row ids.
4. Compute previews only for that union.

- [ ] **Step 3: Load base/source/target rows**

For each row, load:

- latest common ancestor between source and `main`
- current visible source overlay row
- current visible `main` row

- [ ] **Step 4: Reuse merge-strategy logic**

Use existing per-column merge strategy resolution for preview values. Track:

- changed columns
- conflict columns
- row-level diff kind
- row-level errors

- [ ] **Step 5: Return query-shaped rows**

Rows should look like ordinary query rows plus `$diff`. Inserts and updates return the merge preview. Deletes return the `main` row being removed.

- [ ] **Step 6: Add diff tests**

Cover:

- insert diff
- update diff
- delete diff
- unchanged row
- conflict column reporting
- unresolved parent error
- query scope union behavior

---

### Task 9: Implement Branch `.merge()`

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`
- Modify: `packages/jazz-tools/src/runtime/client.ts`
- Modify: `crates/jazz-tools/src/runtime_core/*`
- Modify: `crates/jazz-tools/src/query_manager/writes.rs`
- Modify: `crates/jazz-tools/src/query_manager/manager.rs`
- Modify: `crates/jazz-tools/src/query_manager/bindings.rs`

- [ ] **Step 1: Add public API**

Expose:

```ts
await db.branch(branch.id).merge();
```

No target argument.

- [ ] **Step 2: Snapshot local inputs**

At merge start, snapshot the locally visible source branch tips and `main` tips. Do not wait for remote branch writes.

- [ ] **Step 3: Compute merge outputs**

Use the same base/source/target logic as diff. Diff conflicts do not block merge; merge strategies resolve values.

- [ ] **Step 4: Write results to `main`**

For each changed row, write a normal row-history entry to `main` with parents from:

- current local `main` frontier
- current local source branch frontier

- [ ] **Step 5: Preserve all-or-nothing failure**

If a merge strategy cannot compute a value, fail before writing any rows for that merge batch.

- [ ] **Step 6: Add merge tests**

Cover:

- branch update merges into `main`
- branch insert merges into `main`
- branch delete merges into `main`
- merge excludes remote writes not locally visible at start
- merge writes cross-branch parents
- repeated merge may add history but does not change visible values incorrectly

---

### Task 10: Handle Concurrent Merge Diamonds

**Files:**

- Modify: `crates/jazz-tools/src/row_histories/resolution.rs`
- Modify: `crates/jazz-tools/src/row_histories/mod.rs`
- Modify: `crates/jazz-tools/src/query_manager/manager_tests/branches.rs`

- [ ] **Step 1: Detect equivalent duplicate source contributions**

When multiple `main` frontier tips incorporate the same source branch tip and produce the same user values, treat them as one logical contribution.

- [ ] **Step 2: Protect counter strategies**

Ensure counter merge does not double-apply the same branch delta when concurrent merge outputs create a diamond.

- [ ] **Step 3: Preserve distinct outputs**

If concurrent merges observed different source or target inputs, keep normal row-history merge strategy behavior.

- [ ] **Step 4: Add diamond tests**

Cover:

- two concurrent merges of the same source tip
- counter column does not double count
- LWW columns stay stable
- different source tips still merge as distinct changes

---

### Task 11: Wire Sync And Subscriptions

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/subscriptions.rs`
- Modify: `crates/jazz-tools/src/sync_manager/*`
- Modify: `packages/jazz-tools/src/subscriptions-orchestrator.ts`

- [ ] **Step 1: Sync branch rows normally**

Confirm branch writes are ordinary row batches under the composed branch name and need no special sync payload shape beyond cross-branch parent encoding.

- [ ] **Step 2: Subscribe to overlay dependencies**

Branch overlay subscriptions should wake for both selected branch writes and relevant `main` writes.

- [ ] **Step 3: Keep subscription trace branches understandable**

Expose the public branch id in debug traces where useful, but keep storage branch names in lower-level runtime diagnostics.

- [ ] **Step 4: Add subscription tests**

Cover:

- branch subscription updates after branch write
- branch subscription updates after fallback `main` row changes
- `main` subscription does not update for branch-only writes

---

### Task 12: End-To-End TypeScript Tests

**Files:**

- Add or modify: `packages/jazz-tools/src/runtime/branching.test.ts`

- [ ] **Step 1: Define a minimal app schema**

Use:

- `branches` table for branch ids and permissions
- `projects` table for app scope
- `todos` table for branchable content
- optional `counters` table for counter merge tests

- [ ] **Step 2: Test the expected product flow**

```ts
const { value: branch } = db.insert(app.branches, { ... });
const draft = db.branch(branch.id);
draft.insert(app.todos, { ... });
await db.all(app.todos.where(...)); // main does not see it
await db.all(app.todos.branch(branch.id).where(...)); // branch sees it
await db.all(app.todos.branch(branch.id).where(...).diff());
await draft.merge();
```

- [ ] **Step 3: Test query-level override**

`db.branch(alice.id).all(app.todos.branch(bob.id).where(...))` reads Bob's branch.

- [ ] **Step 4: Test local-only merge snapshot**

Simulate a remote branch write arriving after merge starts and assert it is not included in that merge.

---

### Task 13: Documentation And Release Notes

**Files:**

- Modify: `docs/superpowers/specs/2026-05-14-fine-grained-branching-and-merging-design.md`
- Modify: `docs/superpowers/specs/2026-05-14-fine-grained-branching-and-merging-user-facing.md`
- Modify as needed: package docs and release notes

- [ ] **Step 1: Update docs after implementation**

Adjust the specs if implementation details differ from the plan.

- [ ] **Step 2: Add examples**

Document:

- `db.branch(branch.id)`
- query-builder `.branch(branch.id)`
- `.diff()`
- `.merge()`
- branch permissions through a `branches` table

- [ ] **Step 3: Add migration notes**

Mention any storage compatibility details for cross-branch parent refs.

---

## Suggested PR Stack

1. Branch addressing and TypeScript branch-scoped API.
2. Branch write context and first-write parent selection.
3. Cross-branch parent refs.
4. Branch overlay reads.
5. Query-builder `.branch(...)` and `.diff()`.
6. Merge runtime operation.
7. Permissions and enforcing-runtime behavior.
8. Subscription and sync polish.
9. End-to-end tests and docs cleanup.

Keep each PR small enough to answer one question clearly. The riskiest PRs are cross-branch parent refs and branch overlay reads; land those with the densest Rust coverage.
