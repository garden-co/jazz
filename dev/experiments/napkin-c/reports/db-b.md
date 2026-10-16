# Unit `db-b`: jazz_db core (db.rs, lib, bindings, ordering, and db/ minus db-a)

Source: `crates/jazz/layers/db/src/` — `db.rs`, `lib.rs`, `binding_args.rs`, `binding_codec.rs`,
`binding_codec/publication_type.rs`, `positional_order.rs`, `result_tree.rs`,
`foreground_node_lease.rs`, and every file in `db/` except `peer_connection.rs`,
`node_runtime.rs` and `mutations.rs` (those three belong to db-a).

Output: `src/jazz_db/**`, with these mirror headers:

| Mirror header | What it declares |
|---|---|
| `_ext_db-b.h` | groove / jazz_types / jazz_model / jazz_protocol / jazz_node externs |
| `_ext_db-b-node.h` | db-a `Node` and `PeerConnection` field layouts, mirrored with db-b type names |
| `_ext_db-b-tx.h` | transactions/reads/subscriptions dependencies, plus the db-a node_runtime and mutations mirrors |
| `_ext_db-b-lc.h` | lifecycle/catalogue dependencies |

My files include only `jz_rt.h` and db-b headers. They never include `_ext_db-a.h`,
`db/node_runtime.h`, `db/peer_connection.h` or `db/mutations.h`.

All 22 db-b C files pass `build/check.sh` (`OK (22 files)`).

## Files covered

Rust "non-test" counts come from `build/rust-loc.txt`. Total is `wc -l`.

| Rust file | Rust non-test | Rust total | C file | C lines |
|---|---:|---:|---|---:|
| db.rs | 5765 | 7171 | db.c (+ db.h 813) | 4925 |
| lib.rs | 29 | 49 | lib.c | 3 |
| binding_args.rs | 67 | 94 | binding_args.c | 84 |
| binding_codec.rs | 252 | 822 | binding_codec.c | 314 |
| binding_codec/publication_type.rs | 173 | 189 | binding_codec/publication_type.c | 204 |
| positional_order.rs | 281 | 427 | positional_order.c (+ .h 58) | 308 |
| result_tree.rs | 45 | 71 | result_tree.c (+ .h 31) | 87 |
| foreground_node_lease.rs | 74 | 222 | foreground_node_lease.c | 77 |
| db/catalogue.rs | 168 | 203 | db/catalogue.c | 157 |
| db/channel_endpoint.rs | 273 | 546 | db/channel_endpoint.c | 295 |
| db/config.rs | 72 | 107 | db/config.c | 75 |
| db/first_load.rs | 502 | 705 | db/first_load.c | 398 |
| db/lifecycle.rs | 1303 | 1850 | db/lifecycle.c | 797 |
| db/mutation_errors.rs | 151 | 170 | db/mutation_errors.c | 137 |
| db/read_barrier.rs | 138 | 405 | db/read_barrier.c | 137 |
| db/reads.rs | 1117 | 1457 | db/reads.c | 1118 |
| db/routed_messages.rs | 220 | 301 | db/routed_messages.c | 224 |
| db/row_availability.rs | 457 | 492 | db/row_availability.c | 504 |
| db/subscriptions.rs | 1347 | 1530 | db/subscriptions.c | 1096 |
| db/transactions.rs | 1436 | 1633 | db/transactions.c | 1203 |
| db/view_update_parts.rs | 310 | 386 | db/view_update_parts.c | 374 |
| db/wire_transport.rs | 658 | 767 | db/wire_transport.c | 847 |
| **total** | **14838** | **19597** | | **13364** (+ 2561 header lines) |

Out of scope (assigned to other workers): `cold_settle_attribution.rs`, `db/peer_connection.rs`,
`db/node_runtime.rs` and `db/mutations.rs`.

## Skipped and why

**Test-only code**
- Every `#[cfg(test)]` module and item. This includes the `fail_next_subscription_refresh` /
  `stall_next_subscription_refresh` Db fields and their checks in
  `refresh_subscriptions_for_tables`.
- `testing`-feature items:
  - all `*_for_test` methods
  - `TestUpstreamConnectionHandle` and `ObservedTestTransport`
  - `open_with_receipt*_for_test`
  - `connect_upstream_for_test`, `stage_upstream_message_for_test`,
    `physical_table_identity_for_test`
  - the maintained-subscription size receipts and footprint structs (lifecycle.rs:1434–1830)
  - `accept_relay_subscriber`
  - `set_catalogue_activation_failpoint`
  - `activate_*_for_test` (catalogue.rs)
  - the query-coverage test diagnostics in subscriptions.rs
- `finalize_local_mergeable_commit_for_test` is not cfg-gated in Rust, so it **was** ported.

**`runtime` feature** (not in the browser build, per PORTING.md)
- lifecycle.rs: `declare_upload_root`, `enable_authoritative_scalar_exit_refresh`,
  `apply_trusted_catalogue_snapshot`, `validate_deployment_snapshot`,
  `trusted_catalogue_snapshot`, `use_wall_clock_for_writes`
- catalogue.rs: `validate_schema_activation`

**Other skips**
- `doctest_support` (db.rs 3452–3505): exists only for doc tests. Doc-comment examples are skipped
  everywhere.
- `sync_autopsy` (feature disabled): ported as no-op stubs (`enable`, `clear`, `dump`), matching the
  disabled-feature shims.
- `BINDING_CODEC_GOLDEN_FIXTURE` and the golden test data in binding_codec.rs: test-only.
- `StackSafeFuture` / `stacker` stack growth: the C port is synchronous, so `tick` and `tick_stats`
  call the inner body directly.

## Stubs

The only stubs are the `sync_autopsy` no-ops above. Every other function is translated with all
of its branches, validations and error messages. Calls into other crates are `extern`
declarations in the mirror headers; other units define them.

## Notable mappings

**Generics are collapsed to one C copy each**
- `PositionalOrder<T>` has one type-erased copy.
- The mergeable and exclusive transaction ops have one implementation each over a transaction
  handle. This covers both the owned `Tx` and borrowed `TxRef` impls.
- `Db<S>` is the single concrete `jazz_db_Db`, because storage is already a trait object.

**Async becomes synchronous**
- A future is a `jazz_db_LocalFuture {self, poll, drop}`.
- `.await` on the node `LocalMutex` becomes `SharedNodeState__lock/unlock`. The synchronous
  `borrow()` is `LocalMutexBorrow__borrow(node, caller)`.
- `poll_fn` loops become loops that call `jz_task_run_ready()`.
- A queued `Box::pin(async move { .. })` becomes a heap closure struct. Examples: queued
  transaction ops, and the streaming-upload cleanup future in lifecycle.c.
- `local_executor::block_on` becomes a direct call.

**Channels, ownership and error codes**
- `oneshot` / `mpsc` become `jz_chan` or shared result cells.
- `Rc` becomes `jz_rc_*`, `Weak` becomes a raw pointer, and `RefCell` becomes direct access with a
  borrow flag where reentrancy matters.
- Errors are `jz_err`:
  - `ErrorCode` values are `JAZZ_EC_*`, numbered from 1.
  - `crate::node::Error` variants are `0x500`–`0x508` and convert through `from_node_error` /
    `from_query_error` (`ne()` / `qe()`).
  - `format!` messages are kept verbatim through `jz_err_fmt`, using `*_debug` helpers for `{:?}`.
- `eprintln!` becomes `jazz_std__eprintln(jz_slice)`.

**Mirrors of db-a**
`Node`, `PeerConnection` and the `Db` methods from `mutations.rs` / `node_runtime.rs` are
declared with the same symbol names (`jazz_db__db__node_runtime__Node__*`,
`jazz_db__db__mutations__Db__*`) but with db-b type names. The header file name and type spellings
differ from db-a's headers; the layouts are kept the same.

## Known link-time mismatches with db-a

These only matter at link time, and only if the port were ever linked rather than size-compared.

- db-a's read_barrier mirror uses `jazz_db__read_barrier__*`, with no `db__`. db-b defines
  `jazz_db__db__read_barrier__*`, which follows the module path.
- `clone_for_reserved_transaction`:
  - db-b defines it as `(db, tx_id, Db *out)`.
  - db-a declares it as returning `Db*`.
- `refresh_subscriptions_for_tables`:
  - db-b defines it as `(db, changed, u32 *out)`, which returns the `usize`.
  - db-a declares it without the out-param.
- db-a declares only `accept_subscriber_with_claims_async` on `Node`. db-b also calls the
  synchronous `accept_subscriber*` variants, which `node_runtime.rs` has in Rust.

## Remaining work

None. Every in-scope Rust file is ported and compiles.
