# groove-storage: napkin C port report

Scope: `crates/groove/src/{storage/, records/, large_values.rs, large_values/, chunks.rs, schema.rs, lib.rs}`.
Out of scope (not touched): `queries.rs`, `cold_settle_attribution.rs`.

Every file listed below passes `build/check.sh` (wasm32-wasip1, -Oz). All 11 `.c` files were checked together at the end and passed with no errors.

## Files: Rust non-test LOC vs C LOC

The Rust figures come from `build/rust-loc.txt` (non-test code lines). The C figures are `wc -l` and include headers.

| Rust file | Rust LOC | C files | C LOC |
|---|---:|---|---:|
| records/values.rs | 2479 | records/values.c + values.h | 3333 + 304 |
| records/mod.rs | 1846 | records/mod.c + mod.h | 1820 + 208 |
| records/macros.rs | 849 | records/macros.c + macros.h | 450 + 57 |
| large_values.rs | 4178 | large_values.c + large_values.h | 4223 + 286 |
| large_values/json_syntax.rs | 449 | large_values/json_syntax.c + .h | 322 + 49 |
| chunks.rs | 1327 | chunks.c + chunks.h | 1137 + 177 |
| schema.rs | 447 | schema.c + schema.h | 570 + 117 |
| storage/mod.rs | 2130 | storage/mod.c + mod.h | 1737 + 316 |
| storage/memory.rs | 460 | storage/memory.c | 686 |
| storage/idb.rs | 485 | storage/idb.c + idb.h | 646 + 28 |
| storage/key_codec.rs | 43 | storage/key_codec.c | 53 |
| lib.rs | 11 | (module declarations only, nothing to port) | 0 |
| (mirror header) | - | _ext_groove-storage.h | 80 |
| **Total** | **14704** | | **16599** |

## Skipped, with reasons

- **storage/manifest.rs (317 LOC).** The CSV has no `groove::storage::manifest` rows. It is used only by the native `StorageFactory` path. The `StorageFactory` trait (PathBuf, native) is skipped for the same reason.
- **storage/test.rs** and `storage::conformance`. These are cfg(test / feature "test").
- **records/tests.rs**, `typed_identity_compatibility_proof.rs` and `contained_descriptor_codec_fixture.rs`. These are cfg(test).
- **large_values/lease_proof_tests.rs.** cfg(test).
- **chunks.rs:**
  - `MemoryChunkStorage`, `TestChunkProvider` and `TestChunkProviderControl`: no browser rows.
  - The forwarding `impl ChunkStorage for Rc<S>` has no body, because it is the same handle in C.
- **large_values.rs:**
  - `prepare_streaming(_with_locator)`, `prepare_reusing` and `visit_reachable_chunks`: no browser rows.
  - Test-only helpers.
  - `LargeValueKind` JSON serde lives in values.c, so it is not duplicated.
- **schema.rs:** the serde Serialize/Deserialize derives. The CSV only has rows for Clone, drop, the constructors and `record_schema*`.
- **storage/mod.rs:**
  - The forwarding `impl OrderedKvStorage for Rc<S>` and `for &S` collapse to one `groove_DynStorage` handle.
  - `cache_token` uses the `self` pointer.
- **cfg(test) counters:** `VARIANT_DESCRIPTOR_BUILDS` and `scan_entries_materialized` are not ported.

## Translation notes (single-copy mappings of generics, async)

- **Records generics.** `RecordField` impls are vtables. `nullable_field` and `create_with_encoded_fields` are a single copy driven by an append callback. Serde is hand-written postcard and JSON.
- **large_values generics:**
  - `cdc_ranges` is type-erased.
  - The `LogicalVisitor` trait is a vtable `{wants, leaf, branch, child_len, exact_cover}`.
  - The `TreeStager` closure is a fn pointer plus a ctx.
  - The sync model for blocked reads: walk every node, then return `IvmRuntimeError::EvaluationBlocked`.
- **OrderedKvStorage:**
  - It is a vtable with NULL meaning "use the trait default". The defaults live in the `groove__storage__OrderedKvStorage__*` dispatch functions, as in Rust.
  - `cf`/`key` arguments are borrowed `jz_slice`; value bodies and operation vectors are passed by value.
  - `reopen` (ErasedReopenableStorage) is a vtable slot that consumes the box.
- **StorageScan** is `{self, vt}`: ReadyStorageCursor, MappedStorageCursor, OverlayScanCursor, MemoryStorageCursor.
- **StagedWriteState** keeps the Rc op blocks: `Rc::get_mut` becomes `jz_rc_strong == 1`, and `Rc::try_unwrap` becomes a move-or-clone. It also keeps the lazy point index thresholds (16/64) and `scan_snapshot` over the nested btree index.
- **IdbStorage:**
  - The async mutation-gate Mutex is a flag held for the duration of each call.
  - `read_resident` keeps the read-committed fast path. Its Pending (cold page) branch cannot occur in the sync port.
  - The generation-conflict retry and back-off loops are kept, with each yield mapped to `jz_task_run_ready()`.
  - `ResetIfCancelled` sets `needs_reset` when an error unwinds.
- **OwnedChunkProvider** (sync):
  - Kept: the verified LRU cache, lease stats, admission (`ActiveChunkRequest`), reclamation guard, and the in-flight coalescing registry with per-consumer ChunkWake waiters.
  - A provider future finishes on its first poll. Its registry slot is only found empty on true self re-entry, which returns `ChunkError::Reentrant`.
  - Admission during reclamation is Pending in Rust. Here it registers the waker and returns `Reentrant`.
- **Weak<LayoutStorage>** (OrderedChunkStorage): small static `rc_downgrade`, `rc_upgrade` and `rc_weak_release` helpers in chunks.c. They work on the public `jz_rc_hdr` layout because jz_rt has no Weak API. **Suggest adding `jz_rc_downgrade`, `jz_rc_upgrade` and `jz_rc_weak_release` to the rt.**
- **Error code bases:**

  | Area | Base |
  |---|---|
  | records | 0x1000 |
  | large_values | 0x1100 |
  | ChunkStorageError | 0x1200 |
  | ChunkError | 0x1210 |
  | storage::Error | 0x1300 |
  | MemoryStorageSnapshotError | 0x1320 |

  Transparent errors (`storage::Error::IdbTree` and the IvmRuntimeError wrappers) pass their codes through.

## For the coordinator: mirror mismatches to reconcile

1. **IvmRuntimeError.** I call `groove__ivm__runtime__IvmRuntimeError__evaluation_blocked()` and `__is_evaluation_blocked(const jz_err*)`, declared in `_ext_groove-storage.h`. The ivm-runtime unit exposes a `GROOVE_IVM_EvaluationBlocked` enum and `IvmRuntimeError__simple(u32)` instead, so one of these needs a shim.
2. **ChunkRequest.** Mine (chunks.h) and `_ext_groove-ivm-db.h` use `{u8 object_hash[32]; groove_Locator locator;}`. `_ext_groove-ivm-rt-b.h` declares `jz_bytes locator` plus `groove_ChunkRequest_clone/drop/cmp`. Mine has no drop, because the struct is plain data.
3. **OwnedChunkProvider:**
   - Mine is `{groove_OwnedChunkProviderState *inner}` (jz_rc).
   - `get(p, request, ChunkLease **out)` matches the ivm-db mirror.
   - `get_tracked(p, request, const jz_waker*, ChunkLease **out, bool *publication_metadata_durability)` differs from rt-b's `(p, const ChunkRequest*, ChunkLease *out, bool*)`.
   - rt-b also mirrors a different OwnedChunkProvider `{vt, rc}` and a by-value ChunkLease. Mine is heap-allocated and freed by `groove__chunks__ChunkLease__Drop__drop`.
4. **RecordStore.** Mine is `{const groove_DynStorage *storage; jz_slice column_family; const groove_RecordDescriptor *descriptor}`. The ivm-db mirror takes a DynStorage by value plus `(const char*, len)`, and its KvPair type corresponds to my `groove_KeyValue`.
5. **storage error base.** The ivm-db mirror reserves 0x0400–0x04ff; mine uses 0x1300.
6. **LayoutStorage.** `new` and `new_boxed` return a jz_rc payload via `groove_LayoutStorage **out`, and `as_dyn(const *)`. Both match the ivm-db mirror.
7. **External symbols this unit needs:**
   - `groove__db__LARGE_VALUE_METADATA_CF[]`, owned by the db unit (`src/groove/db/mod.h`).
   - The idb_tree API, mirrored verbatim inside `#ifndef IDB_TREE_H`.
   - The evaluation_session `EvaluationInputs__loaded_chunk` / `install_chunk` / `LoadedChunk__*`, mirrored from their owner's actual signatures.
8. **groove_StrVec** (`Vec<String>`) is defined once, in records/values.h. storage/mod.h reuses it.

## Blocking

Nothing. Every in-scope file is ported and compiles. Linking needs the mirror reconciliation above, plus rt Weak support (or keeping the local helpers).
