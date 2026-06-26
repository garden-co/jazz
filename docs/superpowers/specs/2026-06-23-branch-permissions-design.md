# Branch Permissions & Query APIs — Design

**Date:** 2026-06-23
**Status:** Approved design (pending spec review)
**Reference:** garden-co/jazz PR #917 ("Branch: permissions and query APIs") — used as a behavioral/API reference. This is a _re-derivation_ in jazz2, not a port of the PR diff.

## Goal

Add branch-scoped access control to jazz2's public API as a complete, useful vertical slice: define branch policies, then write and read branch-scoped rows through a branch `Db` view, with deny-by-default enforcement, across all four CRUD actions and all framework adapters.

A "branch" here is a _data branch_ (a git-like branch of rows), backed by a normal application row. This is distinct from jazz2's lower-level storage-branch string; that machinery already exists and is reused, not rebuilt.

## Scope

### In scope

- **TS permissions DSL** (`packages/jazz-tools/src/permissions/index.ts`):
  - Top-level `policy.forBranch(backingTable, ({ $branch, branchPolicy }) => { ... })`.
  - A **table-keyed** `branchPolicy` builder: `branchPolicy.<table>.allowRead / allowInsert / allowUpdate / allowDelete`.
  - A `$branch` proxy exposing backing-row columns as branch refs.
  - **Full CRUD** branch policies (read, insert, update, delete), including the existing `UpdateRuleBuilder` `using` / `withCheck` split.
- **TS runtime** (`packages/jazz-tools/src/runtime/db.ts`): `db.branch(branchId)` returning a branch-scoped `Db` view supporting `.all()`, `.insert()`, `.update()`, `.delete()`.
- **Framework adapters**: `react-core`, `react-native`, `svelte`, `vue` — `useAll` (and re-exports) work against a branch-scoped `Db` view / query so branch reads are reactive.
- **IR / schema-permissions** (`ir.ts`, `schema-permissions.ts`): serialize branch rules (with `branchBackingTable` and branch-ref operands) through to the Rust core.
- **Rust core**: a new `permission_routing` module that resolves the backing branch row, gates access, and dispatches to branch policies; a `BranchRef` policy value; deny-by-default enforcement wired into reads (`policy_filter`), inserts, updates, and deletes (`writes.rs`).
- **Tests**: black-box integration tests (Rust + TS) covering all four actions for the allowed path, the denied path, and branch isolation.

### Out of scope (deferred to later specs)

- Query-level `.branch(branchId)` modifier on query builders (e.g. `db.all(app.todos.where(...).branch(id))`). The slice ships only `db.branch()`.
- `include` / `union` inputs that target a branch.
- Subscriptions semantics specific to branches beyond what `useAll` needs for branch reads.

## Authoritative API

Taken from the PR description (this is the API contract; the stale `.test-tmp` generated fixture, which shows a single-table `branchPolicy` and a table-builder-level `forBranch`, is **superseded** by this shape).

```ts
const app = s.defineApp({
  projects: s.table({ name: s.string(), ownerId: s.string() }),
  branches: s.table({
    projectId: s.ref("projects"),
    name: s.string(),
    ownerId: s.string(),
  }),
  todos: s.table({
    projectId: s.ref("projects"),
    title: s.string(),
    ownerId: s.string(),
  }),
});

const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.where({ ownerId: session.user_id });
  policy.projects.allowInsert.where({ ownerId: session.user_id });

  policy.branches.allowRead.where({ ownerId: session.user_id });
  policy.branches.allowInsert.where({ ownerId: session.user_id });

  policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
    branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
    branchPolicy.todos.allowInsert.where({
      projectId: $branch.projectId,
      ownerId: session.user_id,
    });
    branchPolicy.todos.allowUpdate.where({ projectId: $branch.projectId });
    branchPolicy.todos.allowDelete.where({ projectId: $branch.projectId });
  });
});

// usage
const branch = await db
  .insert(app.branches, { projectId: project.id, name: "Alice's draft", ownerId: userId })
  .wait({ tier: "local" });

const branchDb = db.branch(branch.id);

await branchDb
  .insert(app.todos, { projectId: branch.projectId, title: "Write API docs", ownerId: userId })
  .wait({ tier: "edge" });

const draftTodos = await branchDb.all(app.todos.where({ projectId: branch.projectId }));
```

Key API decisions:

- `forBranch` is **top-level on `policy`**, taking the backing table as its first argument.
- `branchPolicy` is **table-keyed** (`branchPolicy.<table>.allow*`), parallel to the top-level `policy` object.
- `$branch.<column>` references resolve to the backing branch row's column values.

## Branch identity & composition

- A branch is a normal row in a backing table (e.g. `branches`). Its `branch.id` (`ObjectId`) **is** the public branch id passed to `db.branch(...)`.
- `db.branch(branchId)` produces a `Db` view with `userBranch = branchId`. The existing Rust routing composes the logical branch (`<env>/<schemaHash>/<branchId>`) and isolates storage per branch — **this already works** and is unchanged.
- The same `branchId` segment also identifies the backing row on the composed main branch. That row is loaded as the `$branch` context for branch-policy evaluation.

## Data flow

### Insert through a branch view

1. `branchDb.insert(app.todos, {...})` issues a write tagged with the branch.
2. The Rust write path builds a `PermissionRoute` for `(table = todos, branch = branchId, op = insert)`.
3. The route resolves the backing branch row by `branchId` from the registered `forBranch` backing table → `ResolvedBranchRow` (`$branch`).
4. The `todos` branch `insert` policy is evaluated with `$branch.*` refs bound to literals from the backing row. Deny-by-default if the row is missing/unreadable or no matching branch policy exists.

### Read through a branch view

1. `branchDb.all(app.todos.where(...))` issues a query carrying the branch.
2. `policy_filter` selects the branch policy set via `permission_routing`, resolves `$branch`, and gates on the backing row being readable.
3. Rows are filtered by the branch `read` policy `using` expression with `$branch.*` bound to literals.

### Update / delete through a branch view

- Same routing. For updates, `$branch.*` is bindable in **both** the `using` (which rows may change) and `withCheck` (post-image constraint) expressions of `UpdateRuleBuilder`.

## Components

### 1. TS permissions DSL (`permissions/index.ts`)

- Add top-level `forBranch(backingTable, factory)` to the object returned by `definePermissions`.
- Add a table-keyed `branchPolicy` builder producing branch rules of shape `{ table, action, using | withCheck, branchBackingTable }`.
- Add a `$branch` proxy emitting `{ __jazzPermissionKind: "branch-ref", column }`.
- Branch rules flow through the existing `collectRule` channel, tagged so they serialize distinctly from normal rules.

### 2. IR / schema-permissions (`ir.ts`, `schema-permissions.ts`)

- Carry the new `branchBackingTable` field and `branch-ref` operands through serialization into the Rust-facing schema/permissions payload.

### 3. Rust `permission_routing.rs` (new module)

- `PermissionRoute` enum: `Normal` (table policies), `Branch { policy }` (resolved `forBranch` policy), `NoBranchPolicy` (missing-policy rules apply), `Deny` (hard deny). A composed non-main branch never falls back to `Normal`.
- `ResolvedBranchRow<'a>` (aka `BranchPolicyContext`): the backing row exposed as `$branch`.
- `PolicyEvalRefs` with `.with_branch_context(...)`, threading `row_id` + branch context into evaluation.
- `bind_branch_refs`: `BranchRef(column) => Literal(resolve_branch_row_value(column))`, mirroring the existing `SessionRef => Literal` binding at `policy.rs:1275`.

### 4. Rust policy value (`policy.rs`)

- Extend `PolicyValue` (currently `Literal`, `SessionRef(Vec<String>)`) with `BranchRef(String)` (column name), plus its serde variant. Resolution mirrors `resolve_session_value`.

### 5. Rust enforcement wiring

- Reads: `graph_nodes/policy_filter.rs` / `policy_eval.rs` already accept a branch; route through `permission_routing` to pick the branch policy set and bind `$branch`.
- Writes: `writes.rs` consults the route for insert/update/delete before applying.

### 6. TS runtime (`db.ts`) + framework adapters

- `db.branch(branchId)`: returns a `Db` view with `userBranch = branchId` threaded into queries and writes.
- `react-core` / `react-native` / `svelte` / `vue` `use-all`: accept a branch-scoped `Db`/query so branch reads are reactive, matching the small per-adapter threading the PR performs.

## Deny-by-default semantics

On a non-main branch, normal table policies never apply. Access to a branch-scoped row requires **both**:

1. the backing branch row is resolvable **and** readable by the session, **and**
2. a matching branch policy allows the operation, with `$branch` bound.

Any of {no backing row, unreadable backing row, no matching branch policy} → **deny**.

## Testing

Black-box integration tests using the public API only, per `crates/jazz-tools/TESTING_GUIDELINES.md` (read in full before writing Rust tests). No JSON-like schema/permission definitions — build them with the public DSL.

- **Rust** (`query_manager/rebac_tests/`): for each of read / insert / update / delete —
  - _allowed_: the branch owner operates on `todos` inside their branch.
  - _denied_: a session that cannot read the backing branch row gets no rows / a rejected write.
  - _isolation_: branch `todos` do not appear on `main`, and vice versa.
- **TS** (`packages/jazz-tools/tests/` integration): the same matrix through `db.branch(...)`, plus one framework-adapter smoke test that a branch `useAll` reads branch-scoped rows reactively.

## Open questions / risks

- **Backing-row resolution cost**: resolving and read-gating the backing branch row on every branch operation. For the slice, resolve once per query/write; optimization (caching the resolved `$branch`) is deferred.
- **Multiple `forBranch` backing tables**: the PR's router tries each registered backing table (`NotFound` vs `Denied`). The slice targets a single backing table; the router structure should not preclude multiple, but multi-backing-table resolution is not a slice deliverable.
