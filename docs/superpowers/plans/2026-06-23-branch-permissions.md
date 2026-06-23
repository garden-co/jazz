# Branch Permissions & Query APIs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each Task is sized for one subagent.

**Goal:** Add branch-scoped access control to jazz2's public API: define full-CRUD branch policies with `policy.forBranch`, read/write branch-scoped rows through a `db.branch(branchId)` view (incl. framework adapters), enforced deny-by-default in the Rust core.

**Architecture:** A branch is a normal application row; its id is the public branch id. The TS DSL collects branch rules (tagged with a `branchBackingTable`) alongside normal rules and compiles them into a branch-scoped permission structure carried through the IR to the Rust core. A new Rust `permission_routing` module resolves the backing branch row (`$branch`), gates access on its readability, and dispatches to branch policies; `$branch.<col>` refs bind to literal backing-row values exactly as `SessionRef` does today. jazz2's existing storage-branch routing is reused unchanged.

**Tech Stack:** Rust (`crates/jazz-tools`), TypeScript (`packages/jazz-tools`), vitest, `cargo test`, turbo.

**Reference:** garden-co/jazz PR #917. Executors SHOULD pull the reference diff for the corresponding hunks (it is a _guide_, not a copy source — jazz2 has diverged):

```bash
gh pr diff 917 --repo garden-co/jazz > /tmp/pr917.diff
```

**Spec:** `docs/superpowers/specs/2026-06-23-branch-permissions-design.md` — read it before starting.

**Global rules for every task:**

- Black-box integration tests via the public API per `crates/jazz-tools/TESTING_GUIDELINES.md` (read it before writing Rust tests). No JSON-like schema/permission/query literals — build them with the public DSL / `SchemaBuilder`.
- Do NOT rewrite existing passing tests to match new behavior without surfacing it. New behavior gets new tests.
- Commit after each task with a `feat:`/`test:` message. No AI/Claude attribution in commits.
- Build commands: `pnpm build:core`, `pnpm test` (turbo). Per-package commands are given inline per task.

---

## File Structure

**TypeScript (`packages/jazz-tools/src/`)**

- `permissions/index.ts` — add `forBranch`, table-keyed `branchPolicy` builder, `$branch` proxy; extend `Rule` with `branchBackingTable`; compile branch rules into the compiled-permissions structure.
- `schema-permissions.ts` — carry/normalize branch policies for WASM.
- `ir.ts` — branch-ref operand + branch-policy fields in the IR.
- `runtime/db.ts` — `db.branch(branchId)` returning a branch-scoped `Db` view.
- `react-core/use-all.ts`, `react-native/use-all.ts`, `svelte/use-all.svelte.ts`, `vue/use-all.ts` — accept a branch-scoped Db/query.

**Rust (`crates/jazz-tools/src/query_manager/`)**

- `policy.rs` — `PolicyValue::BranchRef(String)` + serde + binding.
- `permission_routing.rs` — **new**: `PermissionRoute`, `ResolvedBranchRow`, `PolicyEvalRefs`, `bind_branch_refs`.
- `mod.rs` — register `permission_routing` module.
- `types/policy.rs` — branch-ref policy-value input variant; branch policy carrier on schema/permissions types.
- `graph_nodes/policy_filter.rs`, `graph_nodes/policy_eval.rs` — route reads through `permission_routing`.
- `writes.rs` — route insert/update/delete through `permission_routing`.
- `schema_manager/encoding.rs` — encode/decode branch policies + branch-ref values.
- `rebac_tests/branch_policies.rs` — **new**: Rust integration tests.

**TS tests**

- `packages/jazz-tools/src/permissions/index.test.ts`, `dsl.test.ts` — DSL unit coverage.
- `packages/jazz-tools/src/schema-permissions.test.ts` — compile/normalize coverage.
- `packages/jazz-tools/src/runtime/db.branch.test.ts` — **new**: `db.branch()` view.
- `packages/jazz-tools/tests/branch-permissions.integration.test.ts` — **new**: end-to-end CRUD matrix.
- `packages/jazz-tools/src/vue/use-all.test.ts` — adapter smoke.

---

## Task 1: TS DSL — `forBranch`, `branchPolicy`, `$branch`

**Files:**

- Modify: `packages/jazz-tools/src/permissions/index.ts` (`Rule` at ~423; `definePermissions`/`buildPolicyContext` at ~619-700; `compileRules` at ~2055)
- Test: `packages/jazz-tools/src/dsl.test.ts`

- [ ] **Step 1: Write the failing test** in `dsl.test.ts`. Build a real app + permissions through the public DSL and assert the compiled output carries branch policies keyed by the backing table.

```ts
import { describe, it, expect } from "vitest";
import * as s from "./index.js"; // match how dsl.test.ts already imports the schema/permissions DSL

describe("forBranch DSL", () => {
  it("collects table-keyed branch policies tagged with the backing table", () => {
    const app = s.defineApp({
      projects: s.table({ name: s.string(), ownerId: s.string() }),
      branches: s.table({ projectId: s.ref("projects"), ownerId: s.string() }),
      todos: s.table({ projectId: s.ref("projects"), title: s.string(), ownerId: s.string() }),
    });

    const permissions = s.definePermissions(app, ({ policy, session }) => {
      policy.branches.allowRead.where({ ownerId: session.user_id });
      policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
        branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
        branchPolicy.todos.allowInsert.where({
          projectId: $branch.projectId,
          ownerId: session.user_id,
        });
      });
    });

    // The compiled permissions expose a branch-scoped section for `todos`
    // backed by `branches`, with read + insert operations present.
    const branchPolicies = (permissions as any).branchPolicies;
    expect(branchPolicies).toBeDefined();
    expect(branchPolicies.branches.todos.select.using).toBeDefined();
    expect(branchPolicies.branches.todos.insert.with_check).toBeDefined();
  });
});
```

> Note: confirm the exact public import surface (`s.defineApp`/`s.definePermissions`) against the top of the existing `dsl.test.ts` and match it. The assertion shape (`branchPolicies[backingTable][table]`) is the contract this task establishes — keep it stable for Task 2.

- [ ] **Step 2: Run the test, verify it fails.**
      Run: `pnpm --filter jazz-tools exec vitest run src/dsl.test.ts -t "forBranch"`
      Expected: FAIL — `policy.forBranch is not a function` (or `branchPolicies` undefined).

- [ ] **Step 3: Implement the DSL.** In `permissions/index.ts`:
  1. Extend `Rule`: `interface Rule { table: string; action: PolicyAction; using?: Condition; withCheck?: Condition; branchBackingTable?: string; }`.
  2. Add a branch-ref value kind. Near the other `__jazzPermissionKind` value types, add `interface BranchRefValue { readonly __jazzPermissionKind: "branch-ref"; readonly column: string; }` and a `isBranchRefCondition`-style guard. Ensure `resolveWhereInput`/`compileCondition` accept it and lower it to a branch-ref operand (see Task 2 for the IR operand).
  3. Add `createBranchContext()` returning a `Proxy` whose `get(_t, prop)` yields `{ __jazzPermissionKind: "branch-ref", column: prop }` for string props.
  4. Add `buildBranchPolicyBuilder(backingTable, relationsByTable, collectRule)` mirroring `buildTablePolicyBuilder` but table-keyed: for each app table it returns an object with `allowRead/allowInsert/allowDelete` and `get allowUpdate()` whose `.where(...)` calls `collectRule({ table, action, using|withCheck, branchBackingTable: backingTable })`. Update actions use the existing `UpdateRuleBuilder`; pass the backing table so its emitted rule carries `branchBackingTable` (extend `UpdateRuleBuilder` constructor with an optional `branchBackingTable` and include it in `toRule()`).
  5. In `buildPolicyContext`, add `context.forBranch = (backingTableBuilder, factory) => { const backingTable = backingTableBuilder.__jazzPermissionTable; factory({ $branch: createBranchContext(), branchPolicy: buildBranchPolicyBuilder(backingTable, relationsByTable, collectRule) }); }`.
  6. In `compileRules`, branch the switch: when `rule.branchBackingTable` is set, write into a separate `branchPolicies[backingTable][table]` `TablePolicies` structure (reuse `emptyTablePolicies`/`mergeOperationPolicy`/`compileCondition`) instead of the normal `compiled[table]`. Return `{ ...compiled-as-today, branchPolicies }` — but keep the existing `CompiledPermissions` return shape additive (attach `branchPolicies` as a non-enumerable or extra field so existing consumers are unaffected). Update `CompiledPermissions` type accordingly.

- [ ] **Step 4: Run the test, verify it passes.**
      Run: `pnpm --filter jazz-tools exec vitest run src/dsl.test.ts -t "forBranch"`
      Expected: PASS.

- [ ] **Step 5: Typecheck + existing permission tests still green.**
      Run: `pnpm --filter jazz-tools exec tsc --noEmit --pretty false`
      Run: `pnpm --filter jazz-tools exec vitest run src/permissions/index.test.ts`
      Expected: PASS (no regressions).

- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/permissions/index.ts packages/jazz-tools/src/dsl.test.ts
git commit -m "feat(permissions): add forBranch/branchPolicy/\$branch DSL"
```

---

## Task 2: TS IR + schema-permissions — carry branch policies to WASM

**Files:**

- Modify: `packages/jazz-tools/src/ir.ts`
- Modify: `packages/jazz-tools/src/schema-permissions.ts`
- Test: `packages/jazz-tools/src/schema-permissions.test.ts`

- [ ] **Step 1: Write the failing test** in `schema-permissions.test.ts`. Compile permissions with a `forBranch` block (as in Task 1), normalize for WASM, and assert the normalized schema carries a branch-policy section with a branch-ref operand.

```ts
it("normalizes branch policies with branch-ref operands for wasm", () => {
  const app = s.defineApp({
    branches: s.table({ projectId: s.ref("projects"), ownerId: s.string() }),
    projects: s.table({ name: s.string(), ownerId: s.string() }),
    todos: s.table({ projectId: s.ref("projects"), title: s.string(), ownerId: s.string() }),
  });
  const permissions = s.definePermissions(app, ({ policy, session }) => {
    policy.branches.allowRead.where({ ownerId: session.user_id });
    policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
      branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
    });
  });

  const normalized = normalizePermissionsForWasm(permissions);
  // The wasm-facing payload exposes branch policies and a branch-ref operand
  // pointing at the backing row column `projectId`.
  const json = JSON.stringify(normalized);
  expect(json).toContain("branch_policies");
  expect(json).toContain("branch_ref");
  expect(json).toContain("projectId");
});
```

> Match the actual exported name (`normalizePermissionsForWasm` exists at `schema-permissions.ts:490`). Confirm whether the wasm field naming is snake_case (`branch_policies`/`branch_ref`) by checking how existing policies are normalized in `normalizePolicyExprForWasm`; keep naming consistent with that convention and update the assertion to match.

- [ ] **Step 2: Run, verify fail.**
      Run: `pnpm --filter jazz-tools exec vitest run src/schema-permissions.test.ts -t "branch policies"`
      Expected: FAIL — no `branch_policies` in output.

- [ ] **Step 3: Implement.**
  1. In `ir.ts`: add a branch-ref operand to the policy-expr IR union (parallel to the session-ref operand) carrying `{ kind: "branch_ref" | "branch-ref", column: string }`. Add an optional `branchPolicies` field to the compiled-permissions IR type: `Record<backingTable, Record<table, TablePolicies>>`.
  2. In `schema-permissions.ts`: in `normalizePermissionsForWasm` (and `mergePermissionsIntoWasmSchema`), read the `branchPolicies` from the compiled permissions, run each operation policy through `normalizePolicyExprForWasm`, and attach under the wasm field name the Rust side expects (see `schema_manager/encoding.rs`; align with Task 3's decoder). Ensure branch-ref operands survive `normalizePolicyExprForWasm` (add a passthrough case).

- [ ] **Step 4: Run, verify pass.**
      Run: `pnpm --filter jazz-tools exec vitest run src/schema-permissions.test.ts -t "branch policies"`
      Expected: PASS.

- [ ] **Step 5: Typecheck + existing schema-permission tests green.**
      Run: `pnpm --filter jazz-tools exec tsc --noEmit --pretty false`
      Run: `pnpm --filter jazz-tools exec vitest run src/schema-permissions.test.ts`
      Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/ir.ts packages/jazz-tools/src/schema-permissions.ts packages/jazz-tools/src/schema-permissions.test.ts
git commit -m "feat(permissions): carry branch policies + branch-ref operands to wasm IR"
```

---

## Task 3: Rust — `PolicyValue::BranchRef` + decode branch policies

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/policy.rs` (`PolicyValue` at :29, `PolicyValueSerde` at :197, binding at :1275-1296)
- Modify: `crates/jazz-tools/src/query_manager/types/policy.rs` (`PolicyValueInput` at :326)
- Modify: `crates/jazz-tools/src/schema_manager/encoding.rs`
- Test: in `policy.rs` (justified internal unit test — decode shape is not observable via public API yet) and `schema_manager/integration_tests/tests/writes.rs`

- [ ] **Step 1: Write the failing test.** In `policy.rs` `#[cfg(test)]`, add a unit test that a `BranchRef` policy value binds to a literal from a backing row (mirrors the existing `SessionRef` binding test). State explicitly in a comment why this is an internal test: branch-ref binding has no public surface until enforcement (Task 5/6) lands.

```rust
#[test]
fn branch_ref_binds_to_backing_row_literal() {
    // INTERNAL TEST (justified): branch-ref value binding has no public API
    // surface until permission_routing enforcement lands in later tasks.
    let value = PolicyValue::BranchRef("projectId".to_string());
    // resolve_branch_row_value returns the backing row's `projectId` literal.
    let bound = bind_branch_value_for_test(&value, &/* backing row exposing projectId = "p1" */);
    assert_eq!(bound, PolicyValue::Literal(Value::Text("p1".into())));
}
```

> Construct the backing row using existing `RowDescriptor`/`encode_row` test helpers (see `rebac_tests.rs` imports). If `bind_branch_value_for_test` doesn't exist yet, this is the seam you create in Task 4; for Task 3 assert only that `PolicyValue::BranchRef` round-trips through serde (`PolicyValueSerde`).

- [ ] **Step 2: Run, verify fail.**
      Run: `cargo test -p jazz-tools query_manager::policy::tests::branch_ref --lib`
      Expected: FAIL — `BranchRef` variant does not exist.

- [ ] **Step 3: Implement.**
  1. `policy.rs`: add `BranchRef(String)` to `PolicyValue` and a matching `PolicyValueSerde::BranchRef { column: String }` with `From` conversions (mirror `SessionRef`).
  2. `types/policy.rs`: add `PolicyValueInput::Branch(String)` and its `From`/`Into` mappings to `PolicyValue::BranchRef`.
  3. `schema_manager/encoding.rs`: decode the wasm `branch_ref` operand (from Task 2) into `PolicyValue::BranchRef`, and decode the `branch_policies` map into the schema/permissions structure (add a `branch_policies` carrier on the relevant schema-permissions type in `types/policy.rs`, keyed `backing_table -> table -> TablePolicies`).

- [ ] **Step 4: Run, verify pass.**
      Run: `cargo test -p jazz-tools query_manager::policy::tests::branch_ref --lib`
      Expected: PASS.

- [ ] **Step 5: Format + check.**
      Run: `cargo fmt -p jazz-tools && cargo check -p jazz-tools`
      Expected: clean.

- [ ] **Step 6: Commit.**

```bash
git add crates/jazz-tools/src/query_manager/policy.rs crates/jazz-tools/src/query_manager/types/policy.rs crates/jazz-tools/src/schema_manager/encoding.rs
git commit -m "feat(core): add BranchRef policy value and decode branch policies"
```

---

## Task 4: Rust — `permission_routing` module

**Files:**

- Create: `crates/jazz-tools/src/query_manager/permission_routing.rs`
- Modify: `crates/jazz-tools/src/query_manager/mod.rs` (register module)
- Test: unit tests inside `permission_routing.rs`

- [ ] **Step 1: Write the failing test** in the new module: `bind_branch_refs` rewrites `BranchRef(col)` operands to `Literal(backing_row[col])` and leaves other operands untouched.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_branch_refs_replaces_branch_ref_with_backing_row_literal() {
        // Build a ResolvedBranchRow exposing projectId = "p1" using existing
        // RowDescriptor/encode_row helpers, then bind an expr that references
        // $branch.projectId and assert it becomes a literal.
        // (See rebac_tests.rs for row-construction helpers.)
    }
}
```

> Fill in the row construction with the same helpers `rebac_tests.rs` uses (`RowDescriptor`, `encode_row`, `Value`). Keep the test black-box at the module's public function boundary (`bind_branch_refs`).

- [ ] **Step 2: Run, verify fail.**
      Run: `cargo test -p jazz-tools query_manager::permission_routing --lib`
      Expected: FAIL — module/file does not exist.

- [ ] **Step 3: Implement** the module. Define:
  - `pub struct ResolvedBranchRow<'a> { pub table_name: &'a TableName, pub row_id: ObjectId, pub descriptor: &'a RowDescriptor, pub content: &'a [u8] }` with a method to read a column value by name (`fn column_value(&self, column: &str) -> Option<Value>`), using existing row-decoding helpers.
  - `pub type BranchPolicyContext<'a> = ResolvedBranchRow<'a>;`
  - `pub enum PermissionRoute<'a> { Normal, Branch { policy: &'a TablePolicies, context: ResolvedBranchRow<'a> }, NoBranchPolicy, Deny }` — a composed non-main branch never yields `Normal`.
  - `pub struct PolicyEvalRefs<'a> { pub row_id: Option<ObjectId>, pub branch_context: Option<&'a BranchPolicyContext<'a>> }` with `fn with_branch_context(self, ctx) -> Self`.
  - `pub fn bind_branch_refs(expr: &PolicyExpr, ctx: &ResolvedBranchRow) -> PolicyExpr` — walk the expr; map `PolicyValue::BranchRef(col) => PolicyValue::Literal(ctx.column_value(col).unwrap_or(Value::Null))`; recurse like the existing `bind_*`/`resolve_*` walkers in `policy.rs` (`policy.rs:1275` is the `SessionRef` analog — follow its structure).
  - A `QueryManager` method (in this module's `impl QueryManager`) `resolve_branch_route(&self, table, branch, op) -> PermissionRoute` that: detects whether `branch` is the composed main branch (reuse the existing branch-composition utilities used by `query.rs`/`types/branch.rs`); if main → `Normal`; otherwise resolves the backing row by branch-id from the `forBranch` backing table; if not found/unreadable → `Deny`; if found but no branch policy for `(table, op)` → `NoBranchPolicy`; else `Branch { policy, context }`.
  - Register `pub mod permission_routing;` in `query_manager/mod.rs`.

  Consult PR #917's `permission_routing.rs` hunk for the exact route resolution and error-categorization shape; re-derive against jazz2's current `QueryManager` API rather than copying verbatim.

- [ ] **Step 4: Run, verify pass.**
      Run: `cargo test -p jazz-tools query_manager::permission_routing --lib`
      Expected: PASS.

- [ ] **Step 5: Format + check.**
      Run: `cargo fmt -p jazz-tools && cargo check -p jazz-tools`
      Expected: clean.

- [ ] **Step 6: Commit.**

```bash
git add crates/jazz-tools/src/query_manager/permission_routing.rs crates/jazz-tools/src/query_manager/mod.rs
git commit -m "feat(core): add permission_routing module for branch policies"
```

---

## Task 5: Rust — enforce branch policies on reads

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs` (branch param already at :160)
- Modify: `crates/jazz-tools/src/query_manager/graph_nodes/policy_eval.rs` (branch at :20)
- Create: `crates/jazz-tools/src/query_manager/rebac_tests/branch_policies.rs`
- Modify: `crates/jazz-tools/src/query_manager/rebac_tests.rs` (add `mod branch_policies;`)
- Test: `rebac_tests/branch_policies.rs`

- [ ] **Step 1: Write the failing tests** (read-path) in `rebac_tests/branch_policies.rs`, following the existing `rebac_tests` harness (`create_query_manager`, `SchemaBuilder`, `permissions`, `pe`, `execute_query`, `get_branch_for_user_branch`). Cover three cases:

```rust
// 1. allowed: owner reads todos inside their branch (projectId matches $branch.projectId).
// 2. denied: a session that cannot read the backing branch row sees zero branch todos
//    (deny-by-default), even though a normal todos read policy would have matched.
// 3. isolation: a todo inserted on the branch does NOT appear when querying `main`,
//    and a `main` todo does NOT appear when querying the branch.
```

Build the schema (projects/branches/todos) and permissions (`policy.branches.allowRead`, `forBranch(branches){ branchPolicy.todos.allowRead.where(projectId == $branch.projectId) }`) with the public Rust builders. Seed a `branches` row, derive the branch via `get_branch_for_user_branch(&qm, &branch_row_id)`, insert todos on that branch, then assert visible rows per session.

- [ ] **Step 2: Run, verify fail.**
      Run: `cargo test -p jazz-tools query_manager::rebac_tests::branch_policies --lib`
      Expected: FAIL — branch reads not yet routed (todos either all-visible or all-denied incorrectly).

- [ ] **Step 3: Implement.** In `policy_filter.rs`/`policy_eval.rs`, when evaluating a non-main branch:
  1. Call `QueryManager::resolve_branch_route(table, branch, Operation::Read)`.
  2. `Deny` → emit no rows. `NoBranchPolicy` → apply missing-policy rule (deny-by-default for reads). `Branch { policy, context }` → bind the `select.using` expr via `bind_branch_refs(.., context)`, then evaluate as a normal read filter (still applying session refs). `Normal` only occurs for main.
  3. Thread the `ResolvedBranchRow` through `PolicyEvalRefs::with_branch_context` so `$branch` literals are available during evaluation.
     Reuse the existing policy evaluation path — only the policy _selection_ and the `bind_branch_refs` pre-pass are new.

- [ ] **Step 4: Run, verify pass.**
      Run: `cargo test -p jazz-tools query_manager::rebac_tests::branch_policies --lib`
      Expected: PASS (all three read cases).

- [ ] **Step 5: No regressions + format.**
      Run: `cargo fmt -p jazz-tools && cargo test -p jazz-tools query_manager::rebac_tests --lib`
      Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add crates/jazz-tools/src/query_manager/graph_nodes/policy_filter.rs crates/jazz-tools/src/query_manager/graph_nodes/policy_eval.rs crates/jazz-tools/src/query_manager/rebac_tests/branch_policies.rs crates/jazz-tools/src/query_manager/rebac_tests.rs
git commit -m "feat(core): enforce branch read policies with deny-by-default"
```

---

## Task 6: Rust — enforce branch policies on insert/update/delete

**Files:**

- Modify: `crates/jazz-tools/src/query_manager/writes.rs`
- Modify: `crates/jazz-tools/src/query_manager/rebac_tests/branch_policies.rs` (add write tests)
- Test: `rebac_tests/branch_policies.rs`

- [ ] **Step 1: Write the failing tests** (write-path), extending `branch_policies.rs`. For each of insert / update / delete:

```rust
// insert allowed: owner inserts a todo on their branch (withCheck projectId == $branch.projectId
//   AND ownerId == session.user_id) -> settles accepted, visible on the branch.
// insert denied: a session that cannot read the backing branch row -> write rejected; row not visible.
// update allowed/denied: owner updates a branch todo (using projectId == $branch.projectId);
//   non-owner update rejected.
// delete allowed/denied: same pattern with allowDelete.
```

Use the public write API used elsewhere in `rebac_tests` (insert via `qm.insert(...)` on the derived branch; updates/deletes via the same public mutation path the other rebac tests use) and assert accepted/rejected settlement + visible state.

- [ ] **Step 2: Run, verify fail.**
      Run: `cargo test -p jazz-tools query_manager::rebac_tests::branch_policies --lib`
      Expected: FAIL — branch writes not routed.

- [ ] **Step 3: Implement.** In `writes.rs`, before applying an insert/update/delete on a non-main branch:
  1. `resolve_branch_route(table, branch, op)`.
  2. `Deny`/`NoBranchPolicy` → reject the write (deny-by-default). `Branch { policy, context }` → `bind_branch_refs` on the relevant expr (`insert.with_check`, `update.using`+`update.with_check`, `delete.using`) then evaluate against the row (post-image for `with_check`, current row for `using`), combined with session refs.
  3. Reject on policy failure; accept otherwise. Reuse the existing write-policy evaluation; only selection + branch binding are new.

- [ ] **Step 4: Run, verify pass.**
      Run: `cargo test -p jazz-tools query_manager::rebac_tests::branch_policies --lib`
      Expected: PASS (all CRUD cases).

- [ ] **Step 5: Full crate tests + format.**
      Run: `cargo fmt -p jazz-tools && cargo test -p jazz-tools --lib`
      Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add crates/jazz-tools/src/query_manager/writes.rs crates/jazz-tools/src/query_manager/rebac_tests/branch_policies.rs
git commit -m "feat(core): enforce branch insert/update/delete policies"
```

---

## Task 7: TS runtime — `db.branch(branchId)` view

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts` (`DbConfig.userBranch` at ~143; `class Db` at ~869)
- Modify: `packages/jazz-tools/src/runtime/client.ts` (branch threading already partly present)
- Test: `packages/jazz-tools/src/runtime/db.branch.test.ts` (new)

- [ ] **Step 1: Write the failing test** in `db.branch.test.ts`. Using the existing in-process test harness for `Db` (mirror `db.dev-mode.test.ts` setup), assert `db.branch(branchId)` returns a Db-like view whose reads/writes carry the branch.

```ts
it("db.branch(id) routes reads and writes to the branch", async () => {
  // set up Db with the projects/branches/todos app + forBranch permissions (Task 1 DSL)
  // insert a project + a branch row on main
  // const branchDb = db.branch(branch.id);
  // await branchDb.insert(app.todos, { projectId, title: "draft", ownerId }).wait({ tier: "local" });
  // branch todo is visible through branchDb.all(...) but NOT through db.all(...) on main
  const onMain = await db.all(app.todos.where({ projectId }));
  const onBranch = await branchDb.all(app.todos.where({ projectId }));
  expect(onMain).toHaveLength(0);
  expect(onBranch).toHaveLength(1);
});
```

> Follow the exact harness/import pattern in `db.dev-mode.test.ts` (it already imports `branch`-related config). Keep the test in-process (`tier: "local"`).

- [ ] **Step 2: Run, verify fail.**
      Run: `pnpm --filter jazz-tools exec vitest run src/runtime/db.branch.test.ts`
      Expected: FAIL — `db.branch is not a function`.

- [ ] **Step 3: Implement** `Db.branch(branchId: string)`: return a lightweight branch-scoped view exposing `all`, `insert`, `update`, `delete` (and `wait`) that delegate to the same underlying client(s) but set the query/write `userBranch`/`branches` to `branchId`. The existing query path already serializes `branches` (db.ts ~2789-2837) and `DbConfig.userBranch` already exists — the view just overrides the branch per call rather than per Db config. Prefer a thin wrapper object over duplicating Db; do not fork connection/broker state.

- [ ] **Step 4: Run, verify pass.**
      Run: `pnpm --filter jazz-tools exec vitest run src/runtime/db.branch.test.ts`
      Expected: PASS.

- [ ] **Step 5: Typecheck + runtime tests green.**
      Run: `pnpm --filter jazz-tools exec tsc --noEmit --pretty false`
      Run: `pnpm --filter jazz-tools exec vitest run src/runtime`
      Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/runtime/db.ts packages/jazz-tools/src/runtime/client.ts packages/jazz-tools/src/runtime/db.branch.test.ts
git commit -m "feat(runtime): add db.branch() branch-scoped view"
```

---

## Task 8: Framework adapters — branch-scoped `useAll`

**Files:**

- Modify: `packages/jazz-tools/src/react-core/use-all.ts`, `react-core/index.ts`
- Modify: `packages/jazz-tools/src/react-native/use-all.ts`, `react-native/index.ts`
- Modify: `packages/jazz-tools/src/svelte/use-all.svelte.ts`, `svelte/index.ts`
- Modify: `packages/jazz-tools/src/vue/use-all.ts`, `vue/index.ts`
- Test: `packages/jazz-tools/src/vue/use-all.test.ts`

- [ ] **Step 1: Write the failing test** in `vue/use-all.test.ts` (vue has an existing `use-all.test.ts` harness — extend it): a `useAll` driven by a branch-scoped Db/query reads branch-scoped rows.

```ts
it("useAll reads from a branch-scoped db", async () => {
  // build app + forBranch permissions + Db; insert a project + branch + a branch todo
  // const branchDb = db.branch(branch.id);
  // const result = useAll(branchDb, app.todos.where({ projectId }));
  // await flush; expect result to contain the branch todo and not main rows
});
```

> Match the existing `vue/use-all.test.ts` setup exactly (it already exercises `useAll`). If `useAll` takes a `Db` as first arg in this package, pass `branchDb`; if it takes a query, pass a branch-scoped query produced by the branch view.

- [ ] **Step 2: Run, verify fail.**
      Run: `pnpm --filter jazz-tools exec vitest run src/vue/use-all.test.ts -t "branch-scoped"`
      Expected: FAIL — branch rows not read (adapter ignores branch).

- [ ] **Step 3: Implement.** Thread the branch through each `use-all` so a branch-scoped `Db` view (Task 7) produces branch-scoped subscriptions/queries. The PR's per-adapter changes are small (a few lines each) — they pass the branch from the Db/query down into the subscription the hook opens. Apply the same minimal threading to all four adapters and keep their public signatures source-compatible (additive only). Update the `index.ts` re-exports only if a new type is exported.

- [ ] **Step 4: Run, verify pass.**
      Run: `pnpm --filter jazz-tools exec vitest run src/vue/use-all.test.ts`
      Expected: PASS.

- [ ] **Step 5: Typecheck all adapters.**
      Run: `pnpm --filter jazz-tools exec tsc --noEmit --pretty false`
      Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add packages/jazz-tools/src/react-core packages/jazz-tools/src/react-native packages/jazz-tools/src/svelte packages/jazz-tools/src/vue
git commit -m "feat(adapters): branch-scoped useAll across react/react-native/svelte/vue"
```

---

## Task 9: End-to-end integration test — full CRUD matrix

**Files:**

- Create: `packages/jazz-tools/tests/branch-permissions.integration.test.ts`

- [ ] **Step 1: Write the integration test** exercising the public API end-to-end (mirror the harness in existing `packages/jazz-tools/tests/` integration tests). One coherent file with the matrix:

```ts
// Setup: app { projects, branches, todos }; forBranch(branches) policies for
// read/insert/update/delete on todos keyed by $branch.projectId (+ ownerId for writes).
//
// allowed path (branch owner):
//   - insert a todo via db.branch(id).insert(...).wait()
//   - read it back via db.branch(id).all(...)
//   - update it via db.branch(id).update(...)
//   - delete it via db.branch(id).delete(...)
// denied path (a second user who cannot read the backing branch row):
//   - all four operations are denied / yield no rows
// isolation:
//   - branch todos never appear on db.all(...) for main, and vice versa
```

Use two sessions/users (the integration harness supports untrusted clients; see existing integration tests). Assert accepted/rejected settlement and visible state.

- [ ] **Step 2: Run, verify fail (or pass).**
      Run: `pnpm --filter jazz-tools exec vitest run tests/branch-permissions.integration.test.ts`
      Expected: All assertions PASS if Tasks 1-8 are correct. If anything fails, fix the relevant layer (do not weaken the test).

- [ ] **Step 3: Full build + test sweep.**
      Run: `pnpm build:core`
      Run: `pnpm --filter jazz-tools test`
      Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add packages/jazz-tools/tests/branch-permissions.integration.test.ts
git commit -m "test: end-to-end branch permissions CRUD matrix"
```

---

## Final verification

- [ ] `cargo fmt --check -p jazz-tools` — clean
- [ ] `cargo test -p jazz-tools query_manager::rebac_tests::branch_policies --lib` — PASS
- [ ] `cargo test -p jazz-tools --lib` — PASS (no regressions)
- [ ] `pnpm --filter jazz-tools exec tsc --noEmit --pretty false` — clean
- [ ] `pnpm --filter jazz-tools test` — PASS
- [ ] `pnpm build:core` — PASS
- [ ] Spec coverage: forBranch/branchPolicy/$branch (T1), IR (T2), BranchRef + decode (T3), permission_routing (T4), read enforcement (T5), write enforcement (T6), db.branch (T7), adapters (T8), e2e (T9). All scope items covered; deferred items (query-level `.branch()`, include/union branch targeting) intentionally excluded.
