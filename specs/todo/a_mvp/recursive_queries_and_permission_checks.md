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
- Add matching recursive relation support to TS permissions DSL (`policy.recursive(...)` + `policy.exists(...)`).
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

## TS Query DSL (Proposed)

Add a generic relation builder namespace:

```ts
import { query } from "jazz-tools";
import { app } from "./schema/app.js";

const reachableTeams = query.recursive({
  start: app.teams
    .where({ kind: "individual", identity_key: session.subject })
    .select({ team: "id" }),
  step: ({ self }) =>
    self
      .join(app.team_team_edges, { left: "team", right: "child_team" })
      .select({ team: "parent_team" }),
  maxDepth: 10, // optional override; default comes from runtime config
  distinctBy: ["team"], // optional; default = all projected columns
});

const readableResourceIds = reachableTeams
  .join(app.resource_access_edges, { left: "team", right: "team" })
  .where({ grant_role: "viewer" })
  .select({ resource: "resource" })
  .distinctBy(["resource"]);
```

Notes:

- `start` and `step` are relation expressions with the same projected shape.
- `step` receives `self` (accumulated rows up to previous level).
- `maxDepth` is optional per-recursion override.
- Recursion semantics are breadth-first by level.

## TS Permission DSL (Proposed)

Keep existing `definePermissions(...)` shape and add recursive helpers:

```ts
import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

type ResourceRoleValue = "viewer" | "editor" | "manager";

export default definePermissions(app, ({ policy, session }) => {
  const reachableTeams = policy.recursive({
    start: policy.teams
      .where({ kind: "individual", identity_key: session.subject })
      .select({ team: "id" }),
    step: ({ self }) =>
      self
        .join(policy.team_team_edges, { left: "team", right: "child_team" })
        .select({ team: "parent_team" }),
    // maxDepth optional (falls back to global default)
  });

  const hasResourceRole = (resource: unknown, role: ResourceRoleValue) =>
    policy.exists(
      reachableTeams
        .join(policy.resource_access_edges, { left: "team", right: "team" })
        .where({
          "resource_access_edges.resource": resource,
          "resource_access_edges.grant_role": role,
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

- This is generic relation recursion, not team-specific.
- `policy.exists(...)` accepts a relation expression (not only table-local `exists.where({...})`).
- Existing `policy.<table>.exists.where(...)` remains supported as sugar.

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

1. `RecursiveRelationNode` runs `start` as level `0` inner graph(s), collects rows.
2. Node sets `seen = distinct(startRows)` and `frontier = seen`.
3. For each level `d = 1..maxDepth`, node:
   - binds `self = seen`,
   - compiles and settles `step(self)` as level `d` inner graph(s),
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
  - `where`, `join`, `select`, `distinctBy`, `recursive`.
- Extend runtime query adapter to emit recursive query payloads.
- Extend permissions DSL compiler:
  - parse relation expressions,
  - compile `policy.exists(relationExpr)` into policy IR for runtime.

### Rust layer

- Add recursive relation representation to query AST/IR.
- Add `GraphNode::RecursiveRelation` (new file under `graph_nodes/`) that orchestrates unrolled per-level child graph execution.
- Hook recursion evaluator into:
  - normal query execution (`all`, `one`, subscriptions),
  - permission read checks (`PolicyFilterNode`),
  - permission write checks (`server_queries` complex clause path).
- Extend policy expression model for relation-backed exists checks.
- Update schema hashing/encoding for new policy/query variants.

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
  - generates recursive relation typing and method signatures.
- `packages/jazz-tools/src/runtime/query-adapter.test.ts`
  - recursive builder JSON translates to expected runtime query payload.
- `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`
  - recursive traversal returns expected rows for:
    - simple chain,
    - branching graph,
    - cycle graph.
  - default depth (10) vs per-query override behavior.
  - override above hard cap rejected.

### Permission DSL compiler tests (TS)

- `packages/jazz-tools/src/permissions/index.test.ts`
  - compiles `policy.recursive + policy.exists(relationExpr)` into policy IR.
  - preserves existing non-recursive rules and OR-merging behavior.
  - validates bad recursive shapes (projection mismatch between `start` and `step`, unknown columns).
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
