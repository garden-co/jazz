# Unit "protocol-types": jazz_protocol + jazz_types + jazz_compression

Crates covered:
- `crates/jazz/layers/protocol/src` (jazz_protocol)
- `crates/jazz/layers/types/src` (jazz_types)
- `crates/jazz-compression/src` (jazz_compression)

C output is under `src/jazz_protocol/`, `src/jazz_types/` and `src/jazz_compression/`. All 18 `.c` files compile with `build/check.sh` and report `OK (18 files)`. Rust line counts are non-test lines from `build/rust-loc.txt`. C line counts are from `wc -l`.

## Files

| Rust file | Rust lines | C file(s) | C lines | Notes |
|---|---:|---|---:|---|
| protocol/protocol.rs | 4022 | protocol.c, protocol.h | 4374, 734 | full port, including postcard serde for every wire type and the JVRR v2 row codec with its descriptor proof cache |
| protocol/wire.rs | 638 | wire.c, wire.h | 570, 253 | wire.h also declares the types of all three wire submodules |
| protocol/wire/channels.rs | 336 | wire/channels.c | 265 | |
| protocol/wire/channel_credit.rs | 362 | wire/channel_credit.c | 302 | |
| protocol/wire/stream_backend.rs | 517 | wire/stream_backend.c | 451 | |
| protocol/authorization_scope.rs | 341 | authorization_scope.c, .h | 298, 59 | |
| protocol/protocol_limits.rs | 54 | protocol_limits.c (+ macros in protocol.h) | 47 | |
| protocol/storage_codec_profile.rs | 19 | (none) | 0 | skipped, see below |
| protocol/lib.rs | 22 | (none) | 0 | module declarations only |
| protocol/protocol/supporting_set_test_oracle.rs | 77 | (none) | 0 | test-only |
| types/ids.rs | 783 | ids.c (+ most of jazz_types.h) | 773 | |
| types/object.rs | 314 | object.c | 351 | |
| types/identity.rs | 298 | identity.c | 259 | mint and derive skipped, see below |
| types/account_registry.rs | 301 | account_registry.c | 20 | registry is server-only; only the shared types and error codes are ported |
| types/account_registry/storage.rs | 150 | (none) | 0 | server-only |
| types/account_registry/codec.rs | 174 | (none) | 0 | server-only |
| types/time.rs | 147 | time.c | 114 | |
| types/postcard_exact.rs | 38 | postcard_exact.c | 45 | |
| types/app_id.rs | 33 | app_id.c | 24 | |
| types/debug_env.rs | 21 | debug_env.c | 29 | |
| types/local_executor.rs | 14 | local_executor.c | 14 | |
| types/lib.rs | 34 | (none) | 0 | module declarations only |
| types/delivery_diagnostics.rs | 44 | (none) | 0 | `cfg(any(test, feature="testing"))` |
| types/legacy_test_future.rs | 85 | (none) | 0 | `cfg(any(test, feature="testing"))` |
| jazz-compression/stream.rs | 460 | stream.c, stream.h | 238, 37 | browser cfg: decode-only ruzstd |
| jazz-compression/lib.rs | 79 | lib.c | 46 | |
| (mirror header) | | _ext_protocol-types.h | 278 | prototypes for the groove, jazz_model and ruzstd symbols this unit calls |
| (types header) | | jazz_types.h | 220 | |

Totals:
- All in-scope Rust: 9,363 lines.
- Rust that is shipped to the browser (excluding skipped, test-only and lib.rs files): about 8,200 lines.
- C: 9,801 lines, of which 1,581 are headers.

## Skipped, and why

- **protocol/storage_codec_profile.rs**: only napi, server and tools call it. It is absent from `browser-coverage.csv`, and its prototypes stay in protocol.h.
- **wire.rs `validate_frame(s)_for_artifact_corpus`**: a `#[doc(hidden)]` fixture/test helper, absent from the CSV.
- **protocol.rs functions absent from the CSV, with no shipped caller**:
  - SchemaLineagePublication: `author_from_*`, `new_genesis_fixture`.
  - PhysicalIdentityManifest: `evolve*`, `identity_slots`, `replace_identity_slots`, `remint_fresh_collisions`, `validate_evolution_to*`.
  - Free functions: `allocate_table_identity`, `allocate_column_identity`, `reconcile_enum_variant_identities`.
  - ProgramFactEntry serde.
- **protocol/supporting_set_test_oracle.rs**: test-only.
- **types/account_registry/{storage,codec}.rs and the registry itself**: server-only.
- **types/identity.rs mint/derive**: jazz_wasm carries its own copy.
- **types/delivery_diagnostics.rs, legacy_test_future.rs**: compiled only under `cfg(testing)`.
- **jazz-compression lz4 and zstd encoder backends**: not compiled in the browser build, so `StreamEncoder` is always `Unavailable`.

## Stubs and approximations

- **`blake3::derive_key`**: the runtime has no derive_key. It is approximated by a hasher fed the context string and then the key material. Code size is similar; the output is not.
- **Mutex poisoning**: branches for `Arc<Mutex<_>>` are omitted, since the port is single-threaded. This covers the "channel credit mutex poisoned" / "credit mutex poisoned" errors, and the `.expect()` in BufferLease's drop.
- **RefCell borrow checks**: elided in AuthorizationScopeRegistry.
- **`Weak<Mutex<ChannelCredits>>`**: emulated by an rc'd `CreditsWeakCell { target }`. ChannelCredits clears `target` when it is dropped; `Weak::upgrade` becomes a NULL check.
- **`try_reserve_exact` failure branch**: "channel reassembly allocation failed" cannot surface, because `jz_vec_reserve` aborts on OOM.
- **`postcard::ser_flavors::Size`**: replaced by encoding into a scratch buffer and measuring it. This affects `encoded_sync_message_len` and `validate_shape_*_size`.
- **`std::env::var` in runtime_transport_compression_features**: on wasm32 it always returns `Err`, so the function returns `default_transport_compression_features()` (`FEATURE_PAYLOAD_ZSTD`, from the ruzstd feature). The env-string match is dead code and was not ported.
- **OrderedChannelBackend::new**: `cfg!(feature="transport-compression-zstd")` is false in the browser, so a negotiated Zstd codec selects 0 (uncompressed outbound).
- **`web_time::Instant`**: replaced by `jz_now_ms()` with millisecond granularity. The timeout's `div_ceil(1_000_000)` on nanoseconds therefore becomes plain milliseconds.
- **postcard::Error Display**: reproduced by one shared helper, `jazz_protocol__wire__postcard_error_display`, which uses postcard's own message texts.

## Notable Rust-to-C mappings

- **`decode_postcard_exact<T>` / `encodes_exactly<T>`**: in Rust these are instantiated per type (SyncMessage, WireFrame and others). C has a single copy driven by a `jazz_types_PcCodec {decode, encode, drop}` vtable. Each type contributes only a static codec constant.
- **`postcard::to_allocvec(&SyncMessage)`**: Rust monomorphises it at least 3 times (allocvec, the `to_extend` pre-sized path and the Size flavour). C has one serializer walking a `jz_pc_w`.
- **Fallible serializers**: those that can fail return `jz_err`. Serde Serialize impls that call `ser::Error::custom` become C functions returning `jz_err`; this applies to VersionRecord, SchemaVersion's JSON and every container holding them.
- **Errors**: a `WireError` travels as a plain `jz_err`, with code = `0x0500 | code<<4 | retry` and the message in `msg`. `TransportError` uses its own codes (Backpressure 0x0580, Failed). `Result<_, String>` maps to `JAZZ_WIRE_STR_ERR` / `JAZZ_PROTO_ERR_MSG`.
- **Error-code ranges**: jazz_types uses 0x0100, jazz_compression 0x0300, jazz_protocol 0x0400 and wire 0x0500.
- **Containers**: all BTreeMap/BTreeSet uses (scheduler channels, encoder/inbound maps, scope registry, aggregate maps) share the runtime's one type-erased B-tree, with small static `jz_btree_desc` tables. The scheduler's `VecDeque<Message>` and the scope waiters use `jz_deque`.
- **Shared state**: `Rc<RefCell<State>>` (scope registry) and `Arc<Mutex<ChannelCredits>>` become `jz_rc` payload pointers.
- **ChannelScheduler::next_chunk_where(impl Fn)**: the closure becomes a `(fn, ctx)` pair. In Rust each caller's closure monomorphises its own copy; C has one.
- **SyncMessage**: 31 variants, a tag plus a union. The uninhabited reserved variants (14, 15) are rejected through `jz_pc_r_variant`.

## Blocking

Nothing is blocking. Every in-scope, browser-reachable Rust file is ported and compiles.
