# INV-STORAGE-30

- Status: now
- Coverage: ✓

## Invariant

Application table and direct-record-store names MUST be unique and case-sensitive, and MUST not use `__groove_*`, `indices`, or `default`; initial and live schema admission rejects them before Groove durable mutation. Every physical backend ingress MUST reject embedded NUL and names exceeding `u16::MAX` UTF-8 bytes before durable mutation, catalogue admission, or live-memory replacement.

## Enforced by (tests)

`groove::db::tests::schema::{reserved_application_storage_names_fail_before_durable_open,application_storage_names_reject_duplicates_and_are_case_sensitive,application_storage_name_length_is_portable_before_open,live_table_registration_cannot_bypass_application_storage_namespace_checks}`; `groove::storage::memory::tests::import_snapshot_rejects_invalid_families_without_replacing_state`

## Implementation

`groove/src/db/{facade,schema_admission}.rs`; `groove/src/storage/{mod,idb,memory}.rs`; `jazz-storage-{rocksdb,sqlite}/src/lib.rs`
