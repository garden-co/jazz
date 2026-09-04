# INV-MV-1

- Status: now
- Coverage: ✓

## Invariant

No state that feeds a maintained view may change without that maintained view observing the change, either as ordinary deltas through the runtime or as an explicit rebuild from authoritative base state. Producer classes include upstream sync apply, local commit finalize, fate application including merge-back/ahead cleanup, subscription registration changes, repair/refetch apply, and recovery. This invariant was made explicit after July 2026 incidents in fallback classification, bulk-load suppression, serve-dirty gating/epoch handling, fated ahead cleanup, and subscriber dirty propagation.

## Enforced by (tests)

`groove::db::tests::query_subscription_matches_one_shot_recompute_under_seeded_interleavings`; `groove::db::tests::graph_subscriptions_match_recompute_under_seeded_interleavings`; `jazz::db::tests::single_upstream_tick_applies_multiple_subscription_updates`; `jazz::node::tests::harness::m3_seeded_sync_interleavings_converge_against_oracle`

## Implementation

`groove/src/ivm/runtime/mod.rs::IvmRuntime::tick_with_params`; `groove/src/db/mod.rs::Database::commit_batch`; `jazz/src/node/ingest.rs::apply_view_updates_in_batch`; `jazz/src/peer.rs::mark_subscriber_connections_dirty`
