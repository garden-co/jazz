# INV-LENS-23

- Status: now
- Coverage: ✓

## Invariant

The storage-epoch-pinned catalogue kernel MUST use only its frozen numeric record kinds `0..=7`; a kind has the exact documented meaning, and an unknown kind MUST fail closed during discovery and reopen. Schema, bootstrap-ready, pending write-pointer, and active-lineage receipt payloads MUST use their documented versioned typed layouts, consume exactly their input, and reject an unknown version, truncation, trailing bytes, or a non-canonical public-schema body before resident catalogue mutation; no JSON fallback is permitted. A pending write-pointer row id MUST equal UUIDv5(pointer schema UUID, revision LE), and duplicate revisions MUST fail rather than overwrite.

## Enforced by (tests)

`jazz::node::tests::harness::{catalogue_kernel_kind_fixture_is_exact_and_closed,dynamic_edge_reopen_fails_closed_on_unknown_catalogue_kernel_kind,catalogue_kernel_payload_corruption_rejects_reopen_before_resident_mutation,noncanonical_catalogue_public_schema_rejects_reopen_before_resident_mutation,pending_catalogue_write_pointer_reopen_requires_deterministic_row_id,pending_catalogue_write_pointer_reopen_rejects_duplicate_revision}`; `jazz::node::codec::catalogue_payload_tests::{catalogue_bootstrap_and_receipt_payloads_have_exact_v1_golden_bytes,catalogue_kernel_payloads_reject_unknown_truncated_and_trailing_bytes,catalogue_schema_payload_rejects_noncanonical_public_schema_json}`

## Implementation

`jazz/src/node/{codec,state/lifecycle}.rs`
