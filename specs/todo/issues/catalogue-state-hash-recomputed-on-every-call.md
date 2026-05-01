# catalogue_state_hash is recomputed from scratch on every call

## What

`SchemaManager::catalogue_state_hash()` collects known schemas, sorts them, encodes each with `encode_schema`, and feeds the bytes into blake3 on every call. It's invoked per `install_transport`, per WS handshake (`routes.rs:1305`), and inside `AuthHandshake::build_handshake_frame`. Under a reconnect storm of N clients the server pays N full catalogue rehashes.

## Priority

medium

## Notes

- The hash only changes when `add_known_schema` / `publish_lens` / permissions publishing mutates catalogue state.
- Fix: cache behind a dirty flag toggled by those mutators. Invalidate on schema/lens/permissions changes; otherwise serve cached hash.
- File: `crates/jazz-tools/src/schema_manager/manager.rs` (~line 495).
