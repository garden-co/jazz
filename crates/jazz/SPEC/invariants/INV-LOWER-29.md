# INV-LOWER-29

- Status: now
- Coverage: ✓

## Invariant

`jazz_schema_versions.physical_mapping` MUST use the sole v1 typed, canonical, exact-consumed binary payload; payload-enum introduction ordinals are always little-endian `u32`, even below 256. Every physical table, column epoch, and recursive enum variant has a permanent authority-allocated global UUID carried by genesis snapshots and lineage publications and included in publication content identity. Mapped compatible entities retain UUIDs; only genuinely new entities receive fresh UUIDs. A dropped, replaced, or incompatible-epoch UUID is retired permanently and MUST NOT be reused by any later table, column, or recursive enum occurrence. Integer table/column/tag values are node-local compression aliases only. Recovery and activation reject nil, duplicate, missing, changed, unknown-version, malformed, noncanonical, trailing, or retired-reused identity metadata; names, documentation, authored order/ordinal, JSON map order, local allocation, and receipt order MUST NOT determine semantic identity.

## Enforced by (tests)

`jazz::node::codec::catalogue_payload_tests::physical_mapping_payload_has_exact_v1_wide_payload_ordinal_fixture`; `jazz::node::codec::catalogue_payload_tests::physical_mapping_payload_rejects_unknown_malformed_trailing_and_noncanonical_forms`; `jazz::protocol::tests::{authority_physical_identity_manifest_is_explicit_and_rejects_uuid_collisions,physical_identity_evolution_never_reuses_retired_epochs_or_subtrees}`; `jazz::node::tests::catalogue_lenses::incompatible_scalar_enum_epoch_activates_and_recovers`; `jazz::node::physical::variant_case_tests::schema_layout_cases_allocate_durably_without_collisions`

## Implementation

`jazz/src/protocol.rs::{PhysicalIdentityManifest,SchemaLineagePublication::author_from_prior}`; `jazz/src/node/codec.rs::{encode_physical_mapping,decode_physical_mapping}`; `jazz/src/node/physical/bindings.rs::validate_physical_mapping_registries`; `jazz/src/node/state/lifecycle.rs::NodeState::open_catalogue_stage`
