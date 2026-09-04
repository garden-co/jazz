# INV-DATA-23

- Status: now
- Coverage: ✓

## Invariant

Transaction/version receipts MUST have one canonical byte spelling: fixed record-field positions and discriminants, canonical author JSON and UUID/HLC encodings, strictly ordered parent `TxId`s, and no malformed, trailing, or alternate postcard encoding. The shared inbound/outbound validator MUST decode and consume every Groove field before infallible access and MUST return an error rather than panic or repair an immutable receipt. Every semantic node ingress MUST repeat validation before filtering, parking, staging, or mutation because `VersionRecord::new` is public and untrusted.

## Enforced by (tests)

`jazz::protocol::tests::version_parent_sets_have_one_sorted_and_deduplicated_receipt_spelling`; `jazz::node::tests::harness::malformed_version_receipts_fail_closed_at_direct_semantic_ingress`; `jazz::wire::tests::transaction_fate_receipt_has_one_canonical_postcard_spelling`; `jazz::wire::tests::transaction_fate_receipt_rejects_trailing_and_noncanonical_bytes`; `jazz::node::tests::exclusive_transactions::receiver_tracks_partial_exclusive_payload_coverage_per_view`; `jazz::node::tests::sync::view_scoped_cardinality_survives_reopen_and_upgrades_to_complete_payload`; `jazz::node::tests::catalogue_lenses::physical_deletion_register_spans_renamed_schemas_and_reopens`; `jazz::node::tests::sync::accepted_fates_maintain_global_current_tables`

## Implementation

`jazz/src/protocol.rs::VersionRecord::{encode,validate_receipt}`; `jazz/src/protocol.rs::SyncMessage::validate_version_carriers`; `jazz/src/wire.rs::{encode_sync_message,decode_frame,decode_sync_message}`; `jazz/src/node/codec.rs::{TransactionRowRecord,HistoryRowRecord,RegisterRowRecord,WireRowRecord}`
