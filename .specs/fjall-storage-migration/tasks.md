# Implementation Tasks

## Phase 1: Rust Storage Engine (`crates/cojson-storage-fjall`)

- [ ] 1. Create `crates/cojson-storage-fjall/` crate with `Cargo.toml` (depends on `fjall`, `thiserror`, `serde`, `serde_json`; add `tempfile` as dev-dependency for tests)
- [ ] 2. Add `cojson-storage-fjall` to `crates/Cargo.toml` workspace members
- [ ] 3. Implement key encoding module (`src/keys.rs`) — `encode_u64`, `encode_u32`, `encode_composite_key`, `decode_u64`, `decode_u32` functions with big-endian byte encoding
- [ ] 4. Implement `FjallStorage` struct (`src/lib.rs`) — `open(path)` constructor that creates the `Database` and opens all 9 partitions (`covalue_by_id`, `covalue_by_row`, `session_by_cv_sid`, `session_by_row`, `transactions`, `signature_after`, `unsynced`, `deleted`, `meta`), plus `close()` method
- [ ] 5. Implement auto-incrementing row ID helpers — `next_covalue_row_id()` and `next_session_row_id()` reading/writing monotonic counters from the `meta` partition
- [ ] 6. Implement `get_co_value(co_value_id)` — point lookup in `covalue_by_id`, decode `rowID` + `header_json` from the value bytes
- [ ] 7. Implement `upsert_co_value(id, header_json)` — check-then-insert pattern: if key exists return existing rowID, otherwise allocate new rowID, write to both `covalue_by_id` and `covalue_by_row`
- [ ] 8. Implement `get_co_value_sessions(co_value_row_id)` — prefix scan on `session_by_cv_sid` using the encoded `co_value_row_id`, decode each value into session fields (`rowID`, `lastIdx`, `lastSignature`, `bytesSinceLastSignature`, `sessionID`)
- [ ] 9. Implement `get_single_co_value_session(co_value_row_id, session_id)` — point lookup using composite key in `session_by_cv_sid`
- [ ] 10. Implement `add_session_update(session)` — upsert into `session_by_cv_sid` (and `session_by_row` if new session), allocating a new session rowID if needed. Return the session rowID
- [ ] 11. Implement `get_new_transaction_in_session(session_row_id, from_idx, to_idx)` — range scan on `transactions` partition from `encode_key(ses, from)` to `encode_key(ses, to)` inclusive
- [ ] 12. Implement `add_transaction(session_row_id, idx, tx_json)` — insert into `transactions` partition
- [ ] 13. Implement `get_signatures(session_row_id, first_new_tx_idx)` — range scan on `signature_after` with prefix filter for the session
- [ ] 14. Implement `add_signature_after(session_row_id, idx, signature)` — insert into `signature_after` partition
- [ ] 15. Implement `mark_co_value_as_deleted(co_value_id)` — insert into `deleted` partition with status `0` (Pending), idempotent
- [ ] 16. Implement `erase_co_value_but_keep_tombstone(co_value_id)` — atomic `fjall::Batch`: look up coValue rowID, iterate and delete non-tombstone sessions + their transactions + signatures, update `deleted` status to `1` (Done)
- [ ] 17. Implement `get_all_co_values_waiting_for_delete()` — iterate `deleted` partition, collect entries where status is `0` (Pending)
- [ ] 18. Implement `track_co_values_sync_state(updates)` — for each update: if synced, remove `{coValueID}\x00{peerID}` from `unsynced`; if unsynced, insert it
- [ ] 19. Implement `get_unsynced_co_value_ids()` — iterate `unsynced` partition, collect distinct coValue IDs by extracting the prefix before `\x00`
- [ ] 20. Implement `stop_tracking_sync_state(co_value_id)` — prefix scan + delete all entries starting with `{coValueID}\x00` from `unsynced`
- [ ] 21. Implement `get_co_value_known_state(co_value_id)` — look up coValue to confirm existence, prefix scan sessions to get `sessionID` + `lastIdx` counters, return structured known state
- [ ] 22. Write Rust unit tests — roundtrip tests for each method: coValue CRUD, session CRUD, transaction range queries, signature queries, deletion workflow (mark → list → erase → verify tombstone preserved), sync tracking, known state

## Phase 2: NAPI Bindings (inside `cojson-core-napi`)

- [ ] 23. Add `cojson-storage-fjall` as path dependency in `crates/cojson-core-napi/Cargo.toml`
- [ ] 24. Create `crates/cojson-core-napi/src/storage/mod.rs` module — define NAPI result structs (`CoValueResult`, `SessionResult`, `TransactionResult`, `SignatureResult`, `KnownStateResult`) with `#[napi(object)]`
- [ ] 25. Implement `AsyncTask` structs for each `FjallStorage` method — `GetCoValueTask`, `UpsertCoValueTask`, `GetCoValueSessionsTask`, `GetSingleCoValueSessionTask`, `GetNewTransactionInSessionTask`, `GetSignaturesTask`, `AddSessionUpdateTask`, `AddTransactionTask`, `AddSignatureAfterTask`, `MarkCoValueAsDeletedTask`, `EraseCoValueButKeepTombstoneTask`, `GetAllCoValuesWaitingForDeleteTask`, `TrackCoValuesSyncStateTask`, `GetUnsyncedCoValueIDsTask`, `StopTrackingSyncStateTask`, `GetCoValueKnownStateTask`
- [ ] 26. Implement `FjallStorageNapi` struct with `#[napi]` — constructor (`new(path)`) wrapping `FjallStorage` in `Arc`, all methods returning `AsyncTask<XxxTask>`, and `close()` method
- [ ] 27. Register the storage module in `crates/cojson-core-napi/src/lib.rs` — `mod storage; pub use storage::*;`
- [ ] 28. Build and verify NAPI binary compiles — `cd crates/cojson-core-napi && cargo build` (verify `FjallStorageNapi` appears in generated `index.d.ts`)

## Phase 3: TypeScript Package (`packages/cojson-storage-fjall`)

- [ ] 29. Create `packages/cojson-storage-fjall/` package — `package.json` (depends on `cojson-core-napi`, `cojson`), `tsconfig.json`, `vitest.config.ts`
- [ ] 30. Implement `FjallClient` class (`src/index.ts`) — implements `DBClientInterfaceAsync` and `DBTransactionInterfaceAsync`, delegates all methods to `FjallStorageNapi` with JSON parse/stringify for headers and transactions
- [ ] 31. Implement `getFjallStorage(path)` factory function — creates `FjallClient`, wraps in `StorageApiAsync`, returns `StorageAPI`
- [ ] 32. Write TypeScript conformance tests (`src/tests/`) — test all `DBClientInterfaceAsync` methods through the full NAPI roundtrip: coValue CRUD, session operations, transaction range queries, signatures, deletion workflow, sync tracking, known state

## Phase 4: Integration

- [ ] 33. Update `packages/jazz-run/src/startSyncServer.ts` — replace `getBetterSqliteStorage(db)` with `getFjallStorage(db)`, update import
- [ ] 34. Update `packages/jazz-run/package.json` — add `cojson-storage-fjall` dependency, remove `cojson-storage-sqlite` dependency (or keep as optional fallback)
- [ ] 35. Run existing `jazz-run` server tests to verify the swap works end-to-end
- [ ] 36. Run existing storage-related integration tests with fjall backend to ensure behavioral parity

## Phase 5: Build System & CI

- [ ] 37. Verify `build:napi` turbo task picks up new code — ensure `cojson-storage-fjall` Rust source is included in the NAPI build inputs in `turbo.json`
- [ ] 38. Add `packages/cojson-storage-fjall` build task to `turbo.json` — standard TypeScript package build depending on `build:napi`
- [ ] 39. Run full CI pipeline locally — `pnpm build:all-packages && pnpm test --watch=false` to verify nothing breaks
