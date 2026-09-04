# INV-SYNC-28

- Status: now
- Coverage: ✓

## Invariant

Before the reconstruction cut, structured-output wire v6 MUST carry recursive reset snapshots and generic typed root/path terminal operations in FIFO logical view updates. Transport fragmentation occurs only below the logical-message boundary; reassembly, validation, and reset replacement complete before semantic patch apply. At the cut this legacy terminal carrier is retired; `INV-SYNC-31..35` specify the reconstruction and post-cut publication contract.

## Enforced by (tests)

`wire_fixtures::wire_message_frame_fixtures_decode_to_expected_messages`; `jazz::db::tests::array_subquery_remote_subscription_hydrates_edge_referenced_child_rows`; `jazz::db::tests::structured_subscription_splices_in_terminal_root_order_after_insert`

## Implementation

`protocol.rs::SyncMessage::ViewUpdate`; `wire.rs::WireMessageFragment`; `db.rs::refresh_subscriptions_in`; Rust/TS terminal codecs
