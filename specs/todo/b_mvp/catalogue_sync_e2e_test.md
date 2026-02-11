# Catalogue Sync E2E Test — TODO (MVP)

Catalogue tests call `process_catalogue_update()` directly rather than pumping through SyncManager. A full end-to-end test with `wire_up_sync()` / `pump_sync()` helpers would exercise the complete flow.

> `crates/groove/src/schema_manager/integration_tests.rs`
