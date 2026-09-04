# INV-SYNC-28

- Status: prov
- Coverage: ✓

## Invariant

The pre-reconstruction implementation carried recursive terminal resets and typed root/path terminal operations in peer `ViewUpdate`s. This is historical scaffolding, not a correctness contract: the carrier is retired by `INV-SYNC-36` and a conformant implementation removes it rather than preserving an authority-output compatibility path.

## Enforced by (tests)

`wire_fixtures::wire_message_frame_fixtures_decode_to_expected_messages`; `jazz::db::tests::array_subquery_remote_subscription_hydrates_edge_referenced_child_rows`; `jazz::db::tests::structured_subscription_splices_in_terminal_root_order_after_insert`

## Implementation

legacy `protocol.rs::SyncMessage::ViewUpdate`; legacy `db.rs::refresh_subscriptions_in`; Rust/TS terminal codecs
