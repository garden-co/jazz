# Unit report: wasm-bindings

Crates `jazz-wasm` (lib.rs, identity.rs), `idb-tree` and `wasm-tracing`, ported to
`src/jazz_wasm/`, `src/idb_tree/` and `src/wasm_tracing/`. Every C file compiles with
`build/check.sh`.

## Rust files covered and line counts

The Rust counts are non-test lines (from `rust-loc.txt`). The C counts come from `wc -l`.

| Rust file | Rust lines | C file(s) | C lines |
|---|---:|---|---:|
| crates/jazz-wasm/src/lib.rs | 2991 | jazz_wasm/lib.c | 4451 |
| | | jazz_wasm/swb.c (the serde_wasm_bindgen derives lib.rs uses) | 692 |
| | | jazz_wasm/wbg.c + wbg.h (wasm-bindgen glue and imports) | 140 + 116 |
| | | jazz_wasm/jazz_wasm.h | 38 |
| crates/jazz-wasm/src/identity.rs | 77 | jazz_wasm/identity.c | 100 |
| crates/idb-tree/src/lib.rs | 1310 | idb_tree/lib.c | 1631 |
| crates/idb-tree/src/page.rs | 332 | idb_tree/page.c | 421 |
| crates/idb-tree/src/web.rs | 220 | idb_tree/web.c | 349 |
| crates/idb-tree/src/store.rs | 128 | idb_tree/store.c | 90 |
| (dep) xxhash-rust 0.8.15 xxh3_64 | n/a | idb_tree/xxh3.c | 195 |
| | | idb_tree/idb_tree.h | 165 |
| crates/wasm-tracing/src/layer.rs | 382 | wasm_tracing/layer.c | 454 |
| crates/wasm-tracing/src/recorder.rs | 59 | wasm_tracing/recorder.c | 149 |
| crates/wasm-tracing/src/lib.rs | 36 | wasm_tracing/lib.c | 137 |
| crates/wasm-tracing/src/config.rs | 54 | wasm_tracing/config.c | 24 |
| | | wasm_tracing/wasm_tracing.h | 103 |
| **Total** | **5589** | | **9255** (plus 477 in the mirror header) |

The mirror header `src/jazz_wasm/_ext_wasm-bindings.h` (477 lines) declares every callee
in groove, jazz_types, jazz_model, jazz_protocol, jazz_node and jazz_db.

## Skipped and why

- **`jazz-wasm/src/bench_probes.rs` and the `bench-probe*` exports**: feature-gated and not
  in the shipped artifact. `bin/` was out of scope by assignment.
- **`#[cfg(test)]` modules** in every file, including `claim_value_from_json` in lib.rs.
- **`idb_tree::store::MemoryPageStore`**: it does not appear in browser-coverage.csv (it is
  native/test only). The rest of store.rs is ported.
- **`cfg(not(target_arch = "wasm32"))` branches**: only the wasm32 branch of
  `configured_wasm_log_level` is ported.
- **`console_error_panic_hook::set_once()`** and the talc `#[global_allocator]`: in the C
  port these are the runtime's allocator and panic. `__wbindgen_start` is empty.

## Added (not 1:1 with a Rust file)

- **`idb_tree/xxh3.c`**: a scalar port of xxhash-rust's XXH3-64, covering the 0–16,
  17–128, 129–240 and long paths with the default secret. The coverage CSV shows
  `xxh3_64_129to240` and `xxh3_64_long_default` ship, and idb-tree checksums pages with it.
- **`wasm_tracing/lib.c` Registry**: `tracing_subscriber::Registry`/`Layered` are replaced
  by a minimal span registry. It keeps a span-id → SpanData map with refcounts, a
  current-span stack, and the clone_span/try_close cascade, so `WasmLayer` sees the same
  `on_new_span`/`on_enter`/`on_close` stream.
- **`jazz_wasm/wbg.{h,c}`**: the wasm-bindgen host surface.
  - Imports cover Reflect, Array, Uint8Array, Promise, closures, `queueMicrotask`, and
    class-instance wrapping.
  - Exports are `__wbindgen_malloc/realloc/free`, the closure trampolines and the spawned
    task runner.
- **`jazz_wasm/swb.c`**: explicit encoders and decoders for each shape that
  `serde_wasm_bindgen` moves:
  - **Walker**: converts JS values into the serde data model.
  - **Externally tagged enums**: `BranchViewBase` and `ReadViewSourceSpec`.
  - **Internally tagged enums** (`kind`, snake_case): `LargeValueUpdate` and
    `LargeValueUpdatePage`.
  - **Structs**: `BranchSelector`, `SnapshotRef` (with `#[serde(default)] dots`) and `TxId`.
  - **Uuid**: encoded as bytes, because the format is not human-readable.
  - **u64**: checked against the safe range.
  - **Serializers**: `WriteState` and `MutationErrorEvent`. `MutationErrorEvent` is
    camelCase, with `TransactionId` as a 16-number array and `latestSettlement`
    tagged `kind: "rejected"`.

## Stubs

None. Every function in scope has its real body. Calls into other units go through the
`_ext_wasm-bindings.h` mirror declarations, named per PORTING.md (for example
`jazz_db__db__mutations__Db__enqueue_insert`).

## Notable mappings

- **ABI**: each `#[wasm_bindgen]` item is a `JZ_EXPORT` with wasm-bindgen's symbol name:
  - free functions use their `js_name`;
  - methods use `<class>_<jsName>`, e.g. `wasmdb_insert` or `wasmtransport_sendWireFrame`;
  - destructors are `__wbg_<class>_free`, and the start function is `__wbindgen_start`.

  Arguments and returns:
  - Owned `String`/`Vec<u8>` arrive as `(ptr, len)`, and the callee owns them.
  - For `Option<Vec<u8>>`/`Option<String>`, `ptr == 0` means None.
  - Scalar options are passed as `(is_some, value)`; `Option<bool>` uses wasm-bindgen's
    `0xFFFFFF` sentinel.
  - `Result` returns use a retptr slot (`wbg_ret_str`/`wbg_ret_val`/`wbg_ret_ptr`).
- **Errors**: `Result<_, JsValue>` is a `jz_err`.
  - A core `jazz::db::Error` keeps the code `ErrorCode + 1` (always below
    `JAZZ_NODE_ERROR_CODE_MAX`) until `to_js_error` turns it into a JS `Error` with a
    `code` property, as `core_error_to_js` does.
  - `JsValue::from_str(&string_error)` sites are recoded to `WBG_ERR_STR`, so a non-core
    error is never mistaken for a core one.
  - Every error string from the Rust is kept verbatim.
- **Async**:
  - **Promises**: `future_to_promise` is ported faithfully. The body runs as a
    `spawn_local` microtask that settles a promise from `wbg_promise_with_resolvers`.
  - **Inner awaits**: these are synchronous calls, per PORTING.md. Three places stand in
    for a `Poll::Pending` that waits on a host callback, using a `wbg_yield()` host
    import: the `SubscriptionRetryTimer` poll, the streaming-abort `oneshot` wait, and the
    JS event-loop turn.
  - **Poll thunks**: `Pin<Box<dyn Future>>` slots (`PendingNativeRead`,
    `WasmPendingSubscription`) are `{poll, state, drop}` thunks, so the Ready/Pending
    branches of `poll_once`, `wasm_read_or_pending` and `subscribe` are all still present.
- **Wakers**: `WasmQueryRuntimeWake` (an `ArcWake` over an unbounded channel plus a
  coalescing `AtomicBool`) is ported as a `jz_chan` plus a flag. The waker sends a unit
  and spawns the drain task that calls back into JS. Both `pending_operation_waker` and
  the tick scheduler's progress wake use it, with `call0` and `call1("immediate")`
  respectively.
- **Streaming mutation lifecycle**: `Open`, `PushInFlight{oneshot}`, `AbortClaimed` and
  `Closed` are ported with an explicit Rc'd oneshot cell. A dropped sender becomes
  `CANCELED`, which reproduces the `completion.await.map_err(streaming_mutation_closed)`
  path.
- **ReadableStream**: the pull and cancel closures, the `highWaterMark: 0` strategy and
  `Reflect.construct(ReadableStream)` are ported. `Abortable`/`AbortHandle` is an Rc'd
  flag, which keeps both the "cancelled" and the "aborted" outcomes. The generic
  `readable_stream_from_stream<St>` has one instantiation in Rust and one copy in C.
- **Generics and vtables**:
  - **idb-tree**: `TreeCore<S>`/`IdbTree<S>` are written once against an
    `idb_tree_PageStore` vtable, with `IndexedDbPageStore` as the shipped implementation.
  - **Storage**: `open_db<S>`/`open_backend_db<S>` take a type-erased
    `groove_BoxedStorage`. `BrowserStorage = IdbStorage<IndexedDbPageStore>` is reached
    through `groove__storage__idb__IdbStorage__open(idb_tree_PageStore, ...)`. That is the
    interface the groove storage unit should export.
  - **Transport**: `WireTransport`/`TickScheduler`/`HostJsonSchemaValidator` impls are
    static vtables.
- **RefCell/Cell/Rc**:
  - `Rc` uses `jz_rc_*`.
  - `RefCell` borrow-flag checking is not reproduced. The C uses plain fields, so the
    Rust borrow bookkeeping is not part of the size.
  - `LocalMutex::lock().await` on `PeerConnection` is a direct call.
- **Postcard**: `WasmOpenDbConfig` is decoded by hand.
  - It is `{identity{node uuid, author via AuthorSubject::from_canonical}, Option<u64>,
    bool, Option<u32>}`.
  - Trailing bytes are ignored, as `postcard::from_bytes` does.
  - `WasmWriteResult` is encoded by hand.
- **wasm-tracing**:
  - **Trace-entry buffer**: the 5000-entry ring and the "dropped" accounting are ported,
    along with the poisoned-mutex messages and the subscriber callback with its
    unsubscribe closure.
  - **Serialization**: entries serialize to JS objects as camelCase with `kind` tags.
  - **Console**: `%c` console formats are ported verbatim.

## Blockers

None.
