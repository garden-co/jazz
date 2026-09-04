# INV-QUERY-6

- Status: now
- Coverage: untested

## Invariant

`RegisterShape` followed by `Subscribe` MUST cause the serving side to attach the usage-site subscription to the matching canonical program instance `(ShapeId, ResolvedReadKey, PolicySharingKey, BindingId)` and respond with a reset-result-set `ViewUpdate`.

## Enforced by (tests)

`jazz::db::tests::db_query_builder_expresses_s1_shaped_filters_and_include_modes` exercises query surface; direct sync loop behavior has NONE-FOUND named unit

## Implementation

`db.rs::Connection::tick` (subscriber branch); `peer.rs::PeerState::rehydrate_query`
