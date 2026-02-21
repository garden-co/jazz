# Unified Query + Policy IR (Clean Slate, No Backcompat) — TODO (MVP)

## Summary

Rebuild query and policy compilation around one shared Rust relation IR.

- TS query DSL and TS policy DSL both lower to declarative IR payloads.
- Rust owns semantic lowering and execution planning.
- `gather` and `hopTo` are first-class relation combinators everywhere (queries, subscriptions, policy exists checks).
- No compatibility layer for legacy or intermediate paths.

This replaces the current split architecture where:

- query recursion is compiled to runtime query JSON (`recursive`) and executed by `RecursiveRelationNode`,
- policy recursion is partially lowered in TypeScript into nested `PolicyExpr` trees.

## Hard Constraints

- No backward compatibility requirement.
- API cleanliness and composability are more important than preserving existing internals.
- Query and policy combinators should compose uniformly.
- Strong test coverage is required before cutover.

## Status Quo (Concrete)

Current state is functionally close but architecturally split:

1. Query path

- `packages/jazz-tools/src/codegen/query-builder-generator.ts` generates `gather`/`hopTo` on app builders.
- `packages/jazz-tools/src/runtime/query-adapter.ts` now emits relation-IR-first payloads (legacy `joins`/`recursive` fields are no longer semantically lowered in TS).
- Rust query IR (`crates/jazz-tools/src/query_manager/query.rs`) has `Query`, `RecursiveSpec`, join specs, and `result_element_index`.
- Rust execution (`crates/jazz-tools/src/query_manager/graph.rs`, `crates/jazz-tools/src/query_manager/graph_nodes/recursive_relation.rs`) lowers supported `relation_ir` shapes and compiles/executes recursive node.
- If `relation_ir` is present but unsupported by Rust lowering, compilation now fails (no silent fallback to legacy query fields).

2. Policy path

- `packages/jazz-tools/src/permissions/index.ts` has a separate relation planner (`RelationPlan`) for `policy.exists(...)`.
- `policy.exists(relation)` now compiles to `PolicyExpr::ExistsRel { rel }` with relation IR emitted from TS (`relationToIr(...)`), including recursive `gather` relations.
- Rust policy runtime (`crates/jazz-tools/src/query_manager/policy.rs`, `.../graph_nodes/policy_filter.rs`) evaluates `PolicyExpr` and recursive `INHERITS`.

3. Resulting problems

- Same user-facing combinators are implemented through different semantic pipelines.
- Recursion semantics are duplicated across TS and Rust.
- Policy relation planning still has a TS-side relation planner (`RelationPlan`) separate from query-builder lowering.
- Harder to guarantee parity across `all`, `subscribeAll`, and permission checks.
- Rust relation IR lowering now covers `Gather` with post-gather join/filter/project composition used by `gather(...).hopTo(...).where(...)`; unsupported relation IR is still rejected rather than silently falling back.

## Desired End State

### 1) One shared Rust relation IR

Introduce a new IR module for relation algebra used by both Query and Policy compilers.

Proposed core shape (conceptual):

- `RelExpr`
  - `TableScan { table }`
  - `Filter { input, predicate }`
  - `Join { left, right, on, join_kind }`
  - `Project { input, columns }`
  - `Gather { seed, step, frontier_key, max_depth, dedupe_key }`
  - `Distinct { input, key }`
  - `Limit/Offset/OrderBy` (query-only envelope)

- `PredicateExpr`
  - comparison/null/in/list/and/or/not
  - value refs: literal, session ref, outer row ref, frontier ref

Validation/canonicalization pass:

- `gather(...)` step normalization must produce same row shape as seed.
- implicit `id` is made explicit as first-class row identity in IR (no ad-hoc `_id` handling spread across layers).

### 2) Policy IR embeds relational existence checks

Policy boolean tree remains, but relation checks use shared relation IR instead of TS-expanded nested policy chains.

Proposed policy shape (conceptual):

- `PolicyExpr2`
  - `Predicate(PredicateExpr)`
  - `ExistsRel { rel: RelExpr }`
  - `Inherits { operation, via_column, max_depth }` (can internally compile to `ExistsRel + Gather`)
  - `And/Or/Not/True/False`

Important: `policy.exists(relation)` carries declarative relation IR to Rust (with `hopTo` already lowered to `Join + Project` in TS), not semantic recursion expansion in TS.

### 3) Shared planning/execution

Rust compiler pipeline:

1. decode IR payload,
2. validate schema/typing,
3. canonicalize (`gather`, id refs, join/project shape),
4. compile to `QueryGraph` nodes,
5. execute for reads/subscriptions and for policy filtering.

`Gather` compiles to dedicated recursive node (bounded unrolling internally), with the same semantics regardless of call site.

## TypeScript API Shape (Target)

Keep public API uniform:

```ts
app.teams.gather({
  start: { team_id: { eq: 1 } },
  step: ({ current }) => app.team_edges.where({ child_team: current }).hopTo("parent_team"),
  maxDepth: 10,
});

policy.exists(
  policy.teams
    .gather({
      start: { team_id: { eq: session.teamId } },
      step: ({ current }) => policy.team_edges.where({ child_team: current }).hopTo("parent_team"),
      maxDepth: 10,
    })
    .hopTo("resource_edges")
    .where({ role: "viewer" }),
);
```

Rules:

- `hopTo` is a normal combinator on relation queries.
- `gather.step` must return the same row shape/table as `start` root.
- Query and policy builders expose equivalent combinators.
- TS does type/syntax validation plus `hopTo -> join/project` lowering; recursive semantics remain in Rust.

## IR Boundary and Ownership

### TypeScript responsibilities

- Build typed AST payloads for query/policy combinators.
- Enforce local shape/type constraints where cheap and ergonomic.
- Lower `hopTo(...)` into `Join + Project` before IR payload emission.
- No recursive semantic lowering (no depth OR-unroll logic in TS).

### Rust responsibilities

- Semantic validation and canonicalization.
- Lowering of `gather` and joins/projects to executable graph structures.
- Recursion execution semantics and dedupe behavior.
- Subscription invalidation/delta behavior.
- Permission evaluation over relation IR.

## Migration Plan (From Current Status Quo)

## Phase 0: Lock scope

- Declare hard cutover: no backcompat shims for old paths.
- Remove feature flags that preserve interim behavior.
- Keep existing tests as temporary guardrails until replaced.

## Phase 1: Add shared Rust IR layer

- Add new relation IR module and serde types.
- Add explicit row identity ref type (`RowIdRef`) so joins/correlation do not depend on implicit string conventions.
- Add policy-v2 IR type that references relation IR (`ExistsRel`).

Exit criteria:

- IR types compile and roundtrip encode/decode tests pass.

## Phase 2: TS builders emit new IR payloads

- Query builders emit relation AST segments rather than partially lowered runtime fields.
- Policy builders emit policy-v2 AST with embedded relation AST.
- Lower `hopTo(...)` in TS to canonical `Join + Project` relation fragments.
- Delete TS recursive policy expansion logic (`buildRecursiveReachableExpr` path).

Exit criteria:

- Builder snapshot tests show declarative IR payloads only.

## Phase 3: Rust validation/canonicalization + planner unification

- Implement validation/canonicalization passes:
  - `Gather` validation + canonicalization
  - explicit row-id handling
- Compile canonicalized relations for query execution and policy existence evaluation through shared planner code.

Exit criteria:

- Same canonical relation plan for equivalent query and policy relation snippets.

## Phase 4: Gather execution unification

- Compile `Gather` into recursive graph node in all contexts.
- Remove any remaining TS/client runtime recursion execution special cases.
- Ensure subscription path uses same graph-level recursion semantics.

Exit criteria:

- `all` and `subscribeAll` parity tests pass for recursive queries.

## Phase 5: Rework policy enforcement on shared IR

- Evaluate `ExistsRel` through relation planner, not nested TS-generated `PolicyExpr` chains.
- Keep `Inherits` as syntax sugar compiled in Rust to relation + recursion checks.
- Ensure both read and write checks use same policy-v2 evaluator path.

Exit criteria:

- Recursive policy checks match expected behavior without TS depth-unroll logic.

## Phase 6: Delete legacy/intermediate code

- Remove old query adapter fields that represent pre-lowered recursion internals.
- Remove policy relation planner code that duplicates relation semantics in TS.
- Remove dead tests tied to old lowering behavior.
- Update docs and code comments to reflect final architecture.

Exit criteria:

- No references remain to legacy recursive lowering path.

## Test Strategy (Required)

## 1) IR correctness

- Serde roundtrip tests for query IR and policy-v2 IR.
- Canonicalization golden tests (`gather`, row-id refs, join/project shape).

## 2) Query behavior

- Unit/integration matrix for:
  - non-recursive `hopTo` (single/multi-hop),
  - recursive `gather` with depth bounds,
  - cycles + dedupe,
  - includes + joins composition,
  - `all` vs `subscribeAll` parity.

## 3) Policy behavior

- `policy.exists(relation)` across non-recursive and recursive relation plans.
- recursive inherits via self-reference and bounded depth.
- read/write parity tests (USING and WITH CHECK semantics).
- fail-closed cases on invalid recursive config.

## 4) Cross-path parity

- Property-style tests: equivalent relation snippets in query and policy compile to equivalent canonical relation plans.
- Regression tests for known recursion bugs/cycle handling.

## 5) Performance safety

- Benchmarks for recursion depth/fanout envelopes (MVP naive acceptable, but bounded and deterministic).
- Guardrail tests for global default and hard cap enforcement.

## Definition of Done

- Query and policy DSL both lower to one shared declarative Rust-owned IR model.
- `hopTo` and `gather` semantics are identical across query and policy contexts.
- No TS semantic recursion lowering remains.
- Recursive execution path is graph-node based for both reads and subscriptions.
- Recursive permission checks run through shared relation planning, not custom TS expansion.
- Legacy/intermediate recursion APIs and code paths are removed.
- Comprehensive tests pass across IR, query, policy, and subscription layers.
