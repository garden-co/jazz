# INV-LARGE-11

- Status: now
- Coverage: ✓

## Invariant

A descriptor or self-describing untyped node envelope MUST select exactly one registered large-value codec before node interpretation. V1 node/descriptor/stored-scalar bytes and hashes are canonical, semantic decode receipts byte-identically re-encode, and unknown, trailing, alternate, or descriptor/node-version-mismatched encodings fail closed before traversal can disclose child locators or mutate lifecycle/accounting state.

## Enforced by (tests)

`groove::large_values::tests::raw_node_selector_rejects_hash_valid_future_branch_before_v1_decode`; `groove::db::tests::batches::future_format_node_is_rejected_before_v1_decode_by_metadata_and_upload_ingress`

## Implementation

`groove/src/large_values.rs::{LargeValueFormat,decode_node_for_format,decode_node_untyped_authenticated}`
