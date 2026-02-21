# Recursive Queries + Recursive Permission Checks — TODO (MVP)

Implement recursive relation support as a generic feature, then use it for recursive permission checks.

This spec is intentionally scoped to a correct MVP that is naive but shippable:

- recursion is evaluated by unrolling levels inside a dedicated `QueryGraph` node,
- each level is evaluated via child query graphs orchestrated by that node,
- recursion stops at a max depth (default 10),
- per-query/per-clause overrides are supported.

## Why This Exists

Adopters need graph reachability patterns (team hierarchies, folder trees, org trees) in both:

- normal queries,
- permission checks.

Current permissions support `INHERITS` and table-scoped `exists.where(...)`, but not general recursive relation expressions.

## Goals

- Add generic recursive query support to TS query DSL.
- Add matching recursive relation support to TS permissions DSL with the same combinators as query DSL.
- Reuse one recursion execution model for reads and permission checks.
- Keep safety guarantees:
  - bounded recursion,
  - deterministic dedupe,
  - fail-closed permission behavior on errors.

## Non-goals (MVP)

- No fixpoint/delta engine for recursive queries yet.
- No recursive SQL syntax for external SQL clients in this phase.
- No provenance/explain API beyond minimal debug counters/logs.
- No attempt to optimize for very high fanout graphs.
- No backwards-compatibility layer for earlier experimental recursive API names.

## TS Query DSL (Proposed)

Use two public combinators:

- `hopTo(relationName)`:
  - a normal query combinator that traverses one relation hop and changes row shape to the related table.
- `gather({ start, step, maxDepth })`:
  - bounded recursive traversal where `step` returns rows in the same shape as the root query.

```ts
import { app } from "./schema/app.js";

const parentTeams = await db.all(
  app.team_team_edges.where({ child_team: myTeamId }).hopTo("parent_team"),
);

const reachableTeams = await db.all(
  app.teams.gather({
    start: { team_id: { eq: myTeamId } },
    step: ({ current }) => app.team_team_edges.where({ child_team: current }).hopTo("parent_team"),
    maxDepth: 10, // optional override; default comes from runtime config
  }),
);

const readableResourceIds = await db.all(
  reachableTeams
    .hopTo("resource_access_edges")
    .where({ grant_role: "viewer" })
    .select({ resource: "resource" }),
);
```

Notes:

- `gather` is a method on table query builders (`app.<table>.gather(...)`), not a separate namespace.
- `step` receives `current` (current frontier identity/value) and returns a query expression.
- `step` **must** return rows with the same shape as the root query/table.
- `hopTo` is relation-typed by codegen (invalid relation names are type errors).
- Recursion semantics are breadth-first by level with dedupe by root identity.

## TS Permission DSL (Proposed)

Permissions use the same recursive combinators (`gather`, `hopTo`) plus `policy.exists(...)`:

```ts
import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

type ResourceRoleValue = "viewer" | "editor" | "manager";

export default definePermissions(app, ({ policy, session }) => {
  const reachableTeams = policy.teams.gather({
    start: { kind: "individual", identity_key: session.subject },
    step: ({ current }) =>
      policy.team_team_edges.where({ child_team: current }).hopTo("parent_team"),
    maxDepth: 10, // optional; falls back to global default
  });

  const hasResourceRole = (resource: unknown, role: ResourceRoleValue) =>
    policy.exists(
      reachableTeams.hopTo("resource_access_edges").where({
        resource,
        grant_role: role,
      }),
    );

  return [
    policy.resources.allowRead.where((r) => hasResourceRole(r.id, "viewer")),
    policy.resources.allowUpdate.where((r) => hasResourceRole(r.id, "editor")),
    policy.resources.allowDelete.where((r) => hasResourceRole(r.id, "manager")),
  ];
});
```

Notes:

- `policy` should expose the same relation/query combinators as `app` for API uniformity.
- `policy.exists(...)` accepts relation/query expressions (not only table-local `exists.where({...})`).

## Execution Model (Naive Unrolled)

`RecursiveRelationNode` is a first-class graph node, similar in orchestration style to `ArraySubqueryNode`:

- it receives upstream tuples,
- it builds and settles level-specific inner graphs,
- it keeps node-local recursion state (`seen`, `frontier`, `level`),
- it emits materialized recursive relation rows as its output tuples.

### Config

- Global default: `recursiveMaxDepthDefault = 10`.
- Global hard cap: `recursiveMaxDepthHard = 64` (guardrail).
- Per-query/per-clause `maxDepth` override:
  - if omitted: use global default,
  - if above hard cap: reject at compile/runtime boundary.

### Algorithm

For one recursive relation:

1. `RecursiveRelationNode` runs root table with `start` as level `0`, collects rows.
2. Node sets `seen = distinct(startRows)` and `frontier = seen`.
3. For each level `d = 1..maxDepth`, node:
   - binds `current = frontier` (MVP may batch values),
   - compiles and settles `step({ current })` as level `d` inner graph(s),
   - compute `next = distinct(stepRows - seen)`,
   - add `next` to `seen`,
   - set `frontier = next`,
   - stop early if `frontier` is empty.
4. Node emits `seen` (or projected output) downstream.

Implementation detail for MVP: keep an unrolled level stack internally in `RecursiveRelationNode` and reuse existing graph execution/settlement for each level.

### Subscription behavior (MVP)

- Recursive subscriptions run in full-recompute mode:
  - any write touching referenced tables marks recursive query dirty,
  - recompute all levels up to stop condition,
  - diff old/new result at output node.
- This is slower than incremental fixpoint but simple and correct.

### Cycle handling

- Cycles are handled by `seen` dedupe.
- Evaluation terminates when either:
  - no new rows are discovered,
  - depth limit is reached.

## Compiler + Runtime Changes

### TypeScript layer

- Extend query builder/typegen with relation expression IR supporting:
  - `where`, `include`, `hopTo`, `gather`.
- Extend runtime query adapter to emit recursive query payloads.
- Extend permissions DSL compiler:
  - parse relation expressions,
  - compile `policy.exists(relationExpr)` into policy IR for runtime using `gather`/`hopTo` forms.

### Rust layer

- Add recursive relation representation to query AST/IR.
- Add `GraphNode::RecursiveRelation` (new file under `graph_nodes/`) that orchestrates unrolled per-level child graph execution.
- Hook recursion evaluator into:
  - normal query execution (`all`, `one`, subscriptions),
  - permission read checks (`PolicyFilterNode`),
  - permission write checks (`server_queries` complex clause path).
- Extend policy expression model for relation-backed exists checks.
- Update schema hashing/encoding for new policy/query variants.

## `hopTo` Design Clarification + Next Steps

The desired design is compositional and uniform:

- `hopTo(...)` is a normal query combinator (not a special-case helper path).
- It must work the same way in:
  - one-shot reads (`db.all`, `db.one`),
  - subscriptions (`db.subscribeAll`),
  - query expressions used inside `gather.step(...)`,
  - permission relation expressions (`policy.*`).
- `gather(...)` must compile to the recursive query graph node in both TS and Rust.  
  No client-side unrolling and no subscription-only behavior differences.

### Desired execution properties

- No runtime special-casing in `db.ts` / `react-native/db.ts` for hop flattening.
- One compilation path: builder JSON -> query adapter -> runtime query IR -> QueryGraph.
- Stable delta semantics for subscriptions (added/removed/updated) for `hopTo` and `gather`.
- Deterministic dedupe by row identity.

### Implementation next steps

1. Remove `hopTo` special-case execution in TS DB runtime and route all paths through query compilation.
2. Introduce a first-class compiled representation for non-recursive `hopTo` in runtime query IR.
3. Lower that representation in Rust query graph compilation (initially via the existing recursive/hop machinery if needed), so it is subscription-safe.
4. Add coverage for:
   - standalone one-hop `hopTo`,
   - multi-hop chains,
   - `hopTo` inside `gather.step`,
   - `subscribeAll` parity with `all`,
   - update propagation across hop edges.
5. Only after this is stable, optionally lower `hopTo` to join+project for a single canonical implementation strategy.

### Status update (current branch)

- `hopTo` runtime special-casing has been removed from TS DB runtimes; `all` and `subscribeAll` both compile through the same query adapter path.
- Non-recursive `hopTo` now lowers to join-chain + projected result element (`result_element_index`) in runtime IR and Rust `QueryGraph`.
- `gather.step(...hopTo(...))` now compiles to the same recursive step join+projection representation (instead of hop-specific recursive metadata).
- Recursive execution now supports object-id correlated closure without requiring hop-specific execution (enabling recursive join-projection steps).
- Permissions DSL now supports `policy.<table>.gather({ start, step, maxDepth })` + `hopTo(...)` with the same step-shape constraints and relation-name resolution model as query DSL.
- Recursive permission compilation now derives step input/output columns from hop-lowered join metadata, matching query-side join-projection lowering semantics.

## Implicit ID + Robust Join Design (Proposal)

To make join-based `hopTo` lowering robust, implicit row IDs must be first-class in query planning.

### Current gap

- We treat `id` as implicit in many APIs, but join planning is descriptor-column based.
- This causes fragility for `... ON edge.parent_id = table.id` style joins.
- Query adapter currently strips qualifiers in several paths, reducing disambiguation power.

### Proposed model

1. **First-class ID column model**
   - Introduce a canonical internal ID column in descriptors (e.g. `_id`) for every table.
   - Keep public TS DSL surface as `id`.
   - Add explicit mapping: public `id` <-> internal `_id`.

2. **Typed column refs in IR**
   - Replace stringly-typed join/filter column fields with a structured `ColumnRef`:
     - optional table/alias qualifier,
     - canonical column name,
     - explicit flag/kind for implicit ID references.
   - Preserve qualifiers through TS -> runtime IR -> Rust compile.

3. **Join key extraction that supports implicit IDs**
   - In join execution, allow key extraction from tuple identity when the ref is implicit ID.
   - Avoid requiring synthetic ID bytes in row payloads just to join on ID.

4. **Ambiguity handling**
   - If a column name is ambiguous across joined tables, require qualified refs.
   - Return explicit compile errors instead of silently picking a column.

5. **Alias/self-join support**
   - Finish alias-aware join compilation so self-joins are safe and deterministic.

### Rollout for robust join-based `hopTo`

1. Land canonical implicit-ID exposure in descriptors and column resolution.
2. Land `ColumnRef` query IR and parser/adapter support.
3. Enable joins on implicit IDs end-to-end with tests.
4. Lower `hopTo` to join+project in both query and policy compilers.
5. Delete any remaining hop-specific execution branches.

## SQL/schema manager integration

- Preserve current behavior for non-recursive policies.
- For recursive policy expressions in this phase:
  - encode in schema/catalogue structures used by runtime,
  - avoid forcing SQL parser parity in the same change if not required by runtime path.
- If SQL emission must include recursive clauses, fail with clear error until parser support lands.

## Permission Safety Semantics

- Permission evaluation remains fail-closed:
  - malformed recursive clause => deny,
  - runtime evaluation error => deny,
  - missing referenced table/column => deny.
- Hitting depth limit is not an error; it is the defined boundary of authorization reachability.

## Tests

All tests below are required for MVP.

### Query DSL tests (TS)

- `packages/jazz-tools/src/codegen/codegen.test.ts`
  - generates `hopTo` and `gather` method signatures/typing.
- `packages/jazz-tools/src/runtime/query-adapter.test.ts`
  - `hopTo`/`gather` builder JSON translates to expected runtime query payload.
- `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`
  - `hopTo` one-level traversal returns expected related rows.
  - `gather` recursive traversal returns expected rows for:
    - simple chain,
    - branching graph,
    - cycle graph.
  - default depth (10) vs per-query override behavior.
  - override above hard cap rejected.

### Permission DSL compiler tests (TS)

- `packages/jazz-tools/src/permissions/index.test.ts`
  - compiles `policy.<table>.gather + policy.exists(relationExpr)` into policy IR.
  - preserves existing non-recursive rules and OR-merging behavior.
  - validates bad recursive shapes (`step` output shape mismatch with root, unknown relations/columns).
- `packages/jazz-tools/src/permissions/type-inference.test.ts`
  - row/session typing works inside recursive permission expressions.
  - invalid table/column references fail at type level where possible.

### Rust query/runtime tests

- `crates/jazz-tools/src/query_manager/graph_nodes/recursive_relation.rs`
  - node-level tests for level orchestration, dedupe, cycle termination, and max-depth stop.
- `crates/jazz-tools/src/query_manager/manager_tests.rs`
  - recursive query correctness with chain/branch/cycle fixtures.
  - recursive subscription full-recompute delta correctness.
  - deterministic dedupe and stable output across repeated ticks.

### Rust permission integration tests

- `crates/jazz-tools/src/query_manager/rebac_tests.rs`
  - recursive read permission allows via ancestor team.
  - recursive read permission denies when path exceeds max depth.
  - per-clause `maxDepth` override allows deeper reachability.
  - recursive write permission checks (INSERT/UPDATE/DELETE) allow/deny correctly.
  - cycle graph does not hang and does not over-grant.

## Rollout Plan

1. Land query-side recursive relation IR + evaluator (read path only).
2. Land TS query DSL surface + integration tests.
3. Land permission DSL recursive relation compilation.
4. Wire recursive relation checks into read/write permission evaluators.
5. Land ReBAC integration tests and docs examples.

## Follow-ups (Post-MVP)

- Incremental recursive fixpoint engine (avoid full recompute subscriptions).
- Dependency-level invalidation for recursive clauses.
- Explain/provenance API for recursive permission decisions.
- Optional strict mode: error when recursion truncates at depth limit.
