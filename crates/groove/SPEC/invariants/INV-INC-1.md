# INV-INC-1

- Status: now
- Coverage: ✓

## Invariant

Incremental delivery invariant (mechanism law). For any maintained view, the work performed to ingest, apply, and publish a change — including snapshot assembly, diffing, and delivery to subscribers — must be bounded by the size of the change and its affected keys, never by the size of the accumulated view state. Corollary 1: Full re-materialization or full-state diffing on a maintained path is a defect, even when its observable output is correct. Equivalence gates (INV-MV-1, the differential oracle) verify observations; INV-INC-1 constrains mechanism; passing the former does not license violating the latter. Corollary 2: A snapshot is not a separate concept: initial hydration is the first delta, applied to empty state, using the same delta shape and delivery pathway as every subsequent change. Any type, wire shape, or code path that exists only to carry the full current state of a maintained view is a second format of the view and shares the burden of proof of the no-second-formats rule. Corollary 3 (strong form, Anselm 2026-07-09): One-shot reads are the degenerate case of maintained views (subscribe, take first delta, unsubscribe) — not the other way around. New query capabilities must define their delta form no later than their one-shot form; a capability shipped one-shot-only is incomplete, not done. Legitimate O(state) moments (initial hydration, reset-after-revocation) are covered: there, the state IS the change.

## Enforced by (tests)

`jazz::incremental_delivery_canary::maintained_relation_include_single_row_changes_are_scale_independent`

## Implementation

`jazz/src/db.rs::refresh_subscriptions_in`; `jazz/src/db.rs::subscription_delta_event`; `jazz/src/node/query_eval.rs::apply_local_maintained_view_transitions`; `jazz/src/node/maintained_subscription_view.rs`
