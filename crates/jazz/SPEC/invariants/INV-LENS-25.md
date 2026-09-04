# INV-LENS-25

- Status: now
- Coverage: ✓

## Invariant

Server-local `cat:` catalogue recovery entries MUST use only the canonical `cat:v1:` plus raw UUID key and exact versioned `JCAT` value layout. The value repeats and must equal its key UUID; metadata keys are strictly byte-ordered; unknown, alternate, malformed, truncated, trailing, or key/value-mismatched bytes MUST fail the whole scan before resident catalogue state is constructed. Nested `ColumnType::Json` schema declarations and `PolicyExpr::ExistsRel` relation trees MUST use only their tagged canonical v1 algebra: every semantic field has a fixed tag, JSON object keys are strictly byte-ordered, and unknown versions/tags, duplicate or unordered keys, noncanonical encodings, truncation, or suffixes MUST reject recovery. A schema payload's decoded hash, `schema_hash` metadata, and entry object ID MUST agree; a lens's source/target metadata and entry object ID MUST agree.

## Enforced by (tests)

`jazz_server::server::catalogue_entry::tests::storage_row_v1_golden_is_exact_and_rejects_alternates`; `jazz_server::server::catalogue_storage::tests::{catalogue_key_v1_is_exact_and_rejects_alternate_spellings,adapter_catalogue_reopens_only_the_v1_default_cf_layout,scan_rejects_one_malformed_entry_before_returning_partial_catalogue}`; `jazz_server::server::catalogue_payload_codec::tests::{nested_catalogue_payload_v1_goldens_are_exact,nested_catalogue_payload_rejects_noncanonical_order_versions_and_suffixes}`; `jazz_server::server::builder::tests::{persistent_builder_fails_when_catalogue_scan_is_corrupt,persistent_builder_rejects_nested_catalogue_codec_corruption_before_recovery}`

## Implementation

`jazz-server/src/server/{catalogue_entry,catalogue_storage,catalogue,catalogue_payload_codec}.rs`
