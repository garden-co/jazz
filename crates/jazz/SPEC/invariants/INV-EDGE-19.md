# INV-EDGE-19

- Status: now
- Coverage: ✓

## Invariant

A dynamically catalogued serving authority MUST NOT accept an uploaded commit unit before an authority has published a permissions head selecting its write schema and table policies. If the head is missing, it MUST reject the unit as `Fate::Rejected(RejectionReason::MalformedCommit("permissions_head_missing: no published permissions head"))`, rather than silently accepting or deferring it.

## Enforced by (tests)

`jazz_tools::catalogue_sync_integration::dynamic_server_keeps_pre_permissions_user_write_hidden_after_publish`; `jazz_tools::edge_server_mode::dynamic_server_publishes_seeded_reachable_policy_and_serves_member_rows`

## Implementation

`jazz/src/db.rs::PeerConnection::tick`; `jazz-server/src/lib.rs::ServerShell::publish_permissions_schema`; `jazz-tools/src/server/runtime_catalogue.rs::publish_runtime_catalogue`
