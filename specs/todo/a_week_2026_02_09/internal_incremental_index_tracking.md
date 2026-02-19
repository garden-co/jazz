# Internal Incremental Index Tracking — TODO (THIS WEEK)

## Context

Incremental query updates currently depend on an additional consumer-side layer (WASM) to reconstruct row order over time. That layer must keep previous order state outside the query engine, which leaks internal complexity and makes correctness harder to guarantee.

## Goal

Move index/order tracking into the query engine's internal incremental algorithm so delta application is self-contained and deterministic.

Consumers should receive enough information to apply updates directly, without tracking prior order themselves.

## Non-goals

- Changing query semantics (`ORDER BY`, `LIMIT`, `OFFSET` behavior stays the same).
- Shipping a new public API surface unrelated to incremental ordering.
- Transport payload compression work (can be a follow-up).

## Required Outcome

- QueryManager (or relevant graph nodes) owns previous-vs-next ordering state for live queries.
- Incremental diffs are produced from internal state transitions, not reconstructed in a downstream adapter layer.
- Consumer code no longer needs to retain "previous ordered ids" to apply deltas correctly.

## Proposed Approach

1. Keep per-subscription internal ordering state keyed by stable row identity.
2. During each mutation tick, compute old/new positions inside the engine while evaluating graph node changes.
3. Emit deltas with explicit positional semantics derived from internal state.
4. Keep `all` as fallback truth, but ensure incremental entries are sufficient for deterministic patching on their own.

## Correctness Invariants

- Applying emitted deltas in order must reconstruct the same result as `all`.
- Reordering caused by updates to sort keys must be represented without consumer-side recomputation.
- `ORDER BY ... LIMIT/OFFSET` edge cases (entering/leaving window, in-window moves) must remain correct.
- Cross-client sync events and local writes must produce equivalent ordering behavior.

## Validation

- Add/extend tests for:
  - insert into middle of ordered set
  - update that changes sort position
  - remove with prior position
  - `LIMIT/OFFSET` window churn
  - mixed add/update/remove in a single tick
- Add regression tests proving consumers can apply deltas without storing prior order.

## Implementation Notes

- Start from existing delta-position work and remove the need for external order-tracking glue.
- Prefer integrating logic near the nodes that own ordering semantics (`Sort`, `LimitOffset`, `Output`) rather than in client adapters.
- Keep migration simple: update in-repo consumers in the same change to verify no previous-order bookkeeping is required.
