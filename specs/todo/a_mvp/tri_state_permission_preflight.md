# Tri-State Permission Preflight - TODO (MVP)

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development`
> or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox
> syntax for tracking.

**Goal:** Add `db.canInsert` and `db.canUpdate` as consultative permission preflight APIs that
return `true`, `false`, or `"unknown"`.

**Architecture:** Reuse the existing boolean row-policy evaluator and write-policy rules, but split
consultative preflight from enforcement. Enforcement keeps failing closed where it does today.
Preflight is evaluated only from the runtime's current local state; it does not request or wait for
edge/global durability and it does not make a server-side authorization decision.

**Tech Stack:** Rust core (`jazz-tools`, WASM, NAPI), TypeScript runtime wrappers, pnpm/turbo test
orchestration.

---

## Why This Exists

Jazz already enforces row policies for local writes, remote user writes, query filters, and magic
columns such as `$canDelete`.

The missing API is a direct way for app code to ask:

- can this session insert this candidate row?
- can this session update this row into this candidate shape?

The answer cannot be only boolean. Complex policies can depend on rows, schema, or permission
metadata that the current runtime has not learned locally yet. For those cases, `false` can mean
"policy says no" or "this runtime does not have enough information to decide". The public result
type includes `"unknown"` for explicit readiness gaps.

`canDelete` is intentionally not part of the public preflight surface. Apps that need delete affordance
state should keep using query permission introspection columns such as `$canDelete`.

## Public API

Add these methods on `Db`:

```ts
export type PermissionDecision = true | false | "unknown";

db.canInsert(table, data): Promise<PermissionDecision>;
db.canUpdate(table, id, patch): Promise<PermissionDecision>;
```

`canInsert` accepts the same table proxy and init shape as `insert`, but it does not accept
`CreateOptions` or a caller-supplied object id in the MVP. It preflights the normal generated-id
insert path only.

`canUpdate` accepts the same table proxy, row id, and partial update shape as `update`.

The methods are consultative snapshots. A `true` result is not a reservation. A later mutation may
still be rejected if the relevant policy data changes before the write is applied or accepted.

## Decision Semantics

Return `true` when the runtime can prove from local data that the policy permits the mutation.

Return `false` when the runtime can prove from local data that the policy denies the mutation.

Return `"unknown"` when the preflight cannot even run the existing policy evaluator because required
local row, schema, or permissions context is unavailable.

Examples that should return `"unknown"`:

- an `update.using` policy needs the old target row, but the row is not locally materializable
- an enforcing runtime is waiting for the current permissions head
- the branch schema needed to interpret the row or policy is not available locally

Examples that should return `false`:

- a simple row/session comparison evaluates to false
- a required explicit policy clause is absent in `RowPolicyMode::Enforcing`
- an anonymous session asks about insert/update
- the relevant policy expression is malformed or refers to an invalid relation in a way the runtime
  can prove is invalid
- the target row is known hard-deleted locally
- the existing boolean policy evaluator returns false, including relation-backed cases where the
  runtime cannot yet distinguish "no matching row" from "matching row not synced"

Input-shape failures should throw, not return a permission decision:

- unknown table
- unknown column in the supplied insert/update shape
- invalid row id string
- invalid value encoding for the schema

Those are API/input errors, not policy answers.

## Operation Rules

### Insert

Evaluate `insert.with_check` against the candidate new row.

The MVP candidate row uses an internally generated row id for evaluation, matching the normal
generated-id insert path. Checking an insert with a caller-specified object id is a later feature.

In `RowPolicyMode::PermissiveLocal`, a missing explicit insert policy permits the insert, matching
current local write behavior.

In `RowPolicyMode::Enforcing`, a missing explicit insert policy denies the insert.

### Update

Evaluate the same two-phase rule as enforcement:

1. `update.using` against the old row.
2. `update.with_check` against the resulting new row.

If both clauses are present, both must be `true`.

If either clause is `false`, the whole result is `false`.

If the old row or authorization context needed to run those checks is unavailable locally, return
`"unknown"` before evaluating the policy. Once the existing boolean evaluator runs, `false` remains
`false` in this MVP.

If no explicit update policy exists:

- `PermissiveLocal`: `true`
- `Enforcing`: `false`

## Local-Only Semantics

Preflight always answers from the runtime's local visible state.

```ts
await db.canUpdate(app.todos, id, { title: "Done" });
```

There is no preflight `tier` option. The tier concept still exists for queries and durable write
settlement, but `canInsert` and `canUpdate` are UI-gating helpers over local state, not remote
authorization requests.

This MVP does not promise to fetch every missing policy dependency on demand. It answers from the
runtime state available after normal sync/query settlement. A later server-side preflight RPC can
improve `"unknown"` into `true` or `false` without changing the public result type.

### Pragmatic MVP Boundary

This MVP does not add a deep tri-state evaluator inside policy expressions. In particular, it does
not audit every `return false` in `PolicyContextEvaluator`, `PolicyGraph`, or relation-backed policy
execution.

That means some theoretical unknowns still return `false` in the MVP:

- missing rows inside `policy.exists(...)`
- missing parent rows inside forward `Inherits`
- incomplete reverse scans inside `InheritsReferencing`
- relation-IR `ExistsRel` graphs that settle false because the runtime lacks rows

That limitation is deliberate. The public result type keeps `"unknown"` now, while deep dependency
completeness is tracked in the follow-up spec:
[Deep Tri-State Policy Unknown](../b_launch/deep_tri_state_policy_unknown.md).

## Implementation Plan

### Task 1: Lock the public TypeScript contract with failing tests

- [ ] Add runtime-facing type tests that assert the public result is exactly
      `true | false | "unknown"`.
- [ ] Add `db.canInsert(app.todos, data)` compile-time coverage using realistic table proxies.
- [ ] Add `db.canUpdate(app.todos, todo.id, patch)` compile-time coverage.
- [ ] Add negative compile-time coverage that `db.canDelete(...)` does not exist.
- [ ] Add negative compile-time coverage that `canInsert` and `canUpdate` do not accept a `tier`
      option.

### Task 2: Add Rust boundary preflight decisions without changing enforcement

- [ ] Add `PermissionPreflightDecision` with `Allow`, `Deny`, and `Unknown`.
- [ ] Add serialization helpers that map to public JS values:
  - `Allow -> true`
  - `Deny -> false`
  - `Unknown -> "unknown"`
- [ ] Keep all existing boolean policy enforcement call sites unchanged.

### Task 3: Add pragmatic unknown readiness checks

- [ ] Return `Unknown` when an enforcing runtime needs the current permissions head and it is not
      available.
- [ ] Return `Unknown` when the branch schema needed to build or decode the candidate row is not
      available.
- [ ] Return `Unknown` when `canUpdate` cannot materialize the target row locally.
- [ ] Continue returning `Deny` for missing explicit policies in `RowPolicyMode::Enforcing`.
- [ ] Continue returning `Deny` for anonymous write preflight.
- [ ] Continue returning `Deny` when the existing boolean policy evaluator returns false.

### Task 4: Add operation-specific preflight in QueryManager and SchemaManager

- [ ] Implement insert preflight by building the candidate row with defaults and evaluating
      `insert.with_check`.
- [ ] Do not thread a public caller-supplied object id through insert preflight. Generate the
      candidate id internally, as ordinary inserts do.
- [ ] Implement update preflight by loading the old row, applying the patch in schema order, then
      evaluating `update.using` and `update.with_check`.
- [ ] Preserve current `PermissiveLocal` and `Enforcing` missing-policy behavior.
- [ ] Return `Deny` for anonymous writes before policy evaluation.
- [ ] Keep malformed input as `QueryError` / thrown JS errors.

### Task 5: Expose RuntimeCore, WASM, and NAPI methods

- [ ] Add `RuntimeCore::can_insert` and `RuntimeCore::can_update`.
- [ ] Add WASM methods:
  - `canInsert(table, values)`
  - `canUpdate(object_id, values)`
- [ ] Add NAPI methods with matching names.
- [ ] Add binding tests that call the methods through TypeScript rather than Rust internals.

### Task 6: Add typed `JazzClient` and `Db` wrappers

- [ ] Extend the `Runtime` interface in `packages/jazz-tools/src/runtime/client.ts`.
- [ ] Add `PermissionDecision`.
- [ ] Add `JazzClient.canInsert` and `JazzClient.canUpdate`.
- [ ] In `Db.canInsert`, reuse `transformInsertInput` and `toInsertRecord`.
- [ ] In `Db.canUpdate`, reuse `transformUpdateInput` and `toUpdateRecord`.

### Task 7: Document consultative and time-sensitive semantics

- [ ] Add docs explaining that `can*` is a snapshot, not a reservation.
- [ ] Document local-only semantics:

```ts
const decision = await db.canUpdate(app.todos, todo.id, patch);
if (decision === "unknown") {
  // The runtime cannot prove allow or deny from available local data.
}
```

- [ ] Explain that actual writes remain authoritative and may still be rejected.

## Reactivity Impact

The one-shot `can*` methods are not reactive.

They should be treated like:

```text
permission decision at time T from local runtime state
```

They should not be treated like:

```text
permission guarantee until the user clicks Save
```
