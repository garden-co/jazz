# Policy Query-Time Constant Folding — TODO (Later)

Some policy checks do not depend on row data and should be evaluated once per query (or once per policy-graph instance) instead of once per row.

## Status Note

This is partially addressed today:

- session-only `PolicyExpr` subtrees can already be simplified once when a `PolicyFilterNode` is constructed
- simple cases like `session.where({ "claims.role": "manager" })` no longer need to be re-evaluated for every row in that filter node

What remains open is the broader design for query-time folding/caching across all policy evaluation paths.

> Status quo background:
>
> - [PolicyFilter lifecycle](../../status-quo/life_of_a_subscription.md#11-read-policy-checks-in-compilation-and-settlement)
> - [Query Manager](../../status-quo/query_manager.md)
> - [TypeScript permissions DSL](../a_mvp/permissions_ts_policy_dsl.md)

## Problem

Today, policy evaluation is still fundamentally row-oriented:

- `PolicyFilterNode` evaluates row visibility tuple-by-tuple
- contextual checks (`EXISTS`, `INHERITS`, `EXISTS REL`) can spawn or recurse into additional policy evaluation work
- some of that work is row-independent, but we still pay evaluation overhead repeatedly

This is correct and simple, but it leaves performance on the table when a policy contains clauses that are constant for the lifetime of a single query settlement.

## Goal

Evaluate row-independent policy subtrees once per query / once per `PolicyFilterNode` / once per ad-hoc `PolicyGraph` instance, then reuse the boolean result or simplified expression during row processing.

The important boundary is:

- optimize within a single compiled query or policy graph instance
- do not introduce a global cross-query cache as the first step

## Examples

Should fold once per query:

```ts
session.where({ "claims.role": "manager" });
```

```ts
allOf([
  { owner_id: session.user_id },
  session.where({ "claims.plan": { in: ["pro", "enterprise"] } }),
]);
```

In the second example, only the `session.where(...)` branch is query-constant; `{ owner_id: session.user_id }` still depends on the current row and must remain per-row.

Should not be folded as query-constant:

- `Cmp`, `Contains`, `In`, `InList` when they read a row column
- `EXISTS` conditions that depend on outer-row bindings
- `INHERITS` checks that require loading related rows
- relation expressions or graph traversals whose truth value changes with the current row

## Candidate Scope

### 1. Expand constant folding beyond the current session-only fast path

Current folding is intentionally narrow. Follow-up work can identify additional row-independent shapes and collapse them earlier.

### 2. Apply folding consistently across all policy entry points

Not every policy is evaluated through the same construction path. Follow-up work should audit:

- top-level `SELECT` `PolicyFilterNode`s
- ad-hoc `PolicyGraph`s created for `EXISTS` and write checks
- recursive parent-policy evaluation paths used by `INHERITS`

### 3. Add constant-result fast paths

When a whole policy simplifies to `True` or `False`, settlement should avoid unnecessary per-row expression dispatch and, where possible, avoid tracking irrelevant policy dependencies.

### 4. Preserve dependency correctness

If folding removes a branch containing `EXISTS`/`INHERITS`, dependency-table tracking must shrink accordingly. If folding keeps a contextual branch, dependency tracking must remain unchanged.

## Non-goals

- changing policy semantics
- introducing arbitrary expression rewriting across row refs / session refs / relation refs
- adding a global memoization layer shared across unrelated queries
- weakening fail-closed behavior for missing claims or unresolved graph-backed checks

## Suggested Work Plan

1. Benchmark current row-by-row overhead for mixed row/session policies.
2. Enumerate which `PolicyExpr` shapes are provably row-independent.
3. Apply the same simplification pass at every policy evaluation entry point.
4. Add explicit fast paths for fully constant `True` / `False` policies.
5. Measure impact on:
   - query settle time
   - rows/sec under permission-heavy scans
   - number of contextual policy evaluations per query

## Success Criteria

- session-only or otherwise row-independent policy branches are evaluated once per query instance
- mixed policies still preserve row-by-row correctness for row-dependent clauses
- dependency invalidation behavior remains correct
- benchmarks show measurable improvement for permission-heavy reads without making the policy runtime much harder to reason about
