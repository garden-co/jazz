# Unit `db-a`: jazz_db peer connection, node runtime and mutations

Source: `crates/jazz/layers/db/src/db/{peer_connection,node_runtime,mutations}.rs`.
Output: `src/jazz_db/db/{peer_connection,node_runtime,mutations}.{c,h}` and the mirror header
`src/jazz_db/_ext_db-a.h`.

All three C files compile with `build/check.sh`, with no errors. They also compile with no
warnings under `-Wuninitialized -Wsometimes-uninitialized`.

## Files covered (Rust non-test lines vs C lines)

"Non-test" means the Rust file minus its trailing `#[cfg(test)]` module(s), as counted in
`build/rust-loc.txt`.

| Rust file | Rust non-test | Rust total | C file | C lines | C header |
|---|---:|---:|---|---:|---:|
| peer_connection.rs | 5898 | 7015 | peer_connection.c | 5844 | 186 |
| node_runtime.rs | 5362 | 6354 | node_runtime.c | 5189 | 249 |
| mutations.rs | 3340 | 3831 | mutations.c | 2897 | 102 |
| **total** | **14600** | | | **13930** | 537 |

Mirror header `_ext_db-a.h`: 1631 lines. It declares the types and functions from other units
that this unit calls: groove, jazz_types, jazz_model, jazz_protocol, jazz_node, and the rest of
jazz_db, which includes db.rs, row_availability.rs, read_barrier.rs, routed_messages.rs,
mutation_errors.rs and wire.rs. It includes no other unit's headers.

## What was ported

- **peer_connection.rs**
  - Everything shipped in the browser build. This includes the full `PeerConnection::tick` /
    `tick_inner`, which is about 3700 Rust lines. It is ported as plain synchronous C,
    split into static phase functions:
    - Upstream link: `upstream_outbound` (auxiliary pump, session claims, upload outbox with
      large-value chunk uploads, read barrier, Subscribe/Unsubscribe/AuthorizationScopeIntent
      commands) and `upstream_inbound`.
    - `upstream_inbound` covers chunk upload results, catalogue snapshot,
      current-rows receipt, view updates with catch-up/missing-body resend, subscribe
      rejection, authorization-scope views, aggregate receipts (including the retry-id
      reissue), unavailable and decision messages, the generic apply path, and the batch
      publication/refresh boundary.
    - Subscriber link: `subscriber_prelude`, `subscriber_inbound` (one helper per Rust match arm:
      chunk requests, current rows, scope intent, RegisterShape, Subscribe admission,
      Unsubscribe, upload/other) and `subscriber_serve` (coverage-group serving with initial
      openings, reconcile-for-clone, settled handoff and incremental fan-out).
  - Also ported: the helpers around tick, including authorization-scope serving,
    `apply_pending_authority_view_updates`, the send/flush helpers, relay keys and
    claim rebind.
- **node_runtime.rs**: subscription refresh (detached, structured, flat and ordered-window
  paths), write-state waiters, relay upstream owner registry, shape-rejection routing, and the
  `Transport` trait wrappers. The default trait methods are used when a vtable slot is NULL.
- **mutations.rs**: insert/update/delete/upsert write paths, their validation and error messages,
  and the staged large-value paths that the browser uses.

## Skipped, and why

- **Test-only code.** All `#[cfg(test)]` modules, and in tick the `cfg(any(test, feature = "testing"))`
  `delivery_diagnostics` records and the injected refresh failure.
- **sync-autopsy / cold-settle feature blocks.** These are off in the shipped browser build.
- **peer_connection.rs.** These are test, testing or runtime-feature only:
  - `set_incomplete_receive_timeout_for_test`
  - `scope_relay_*_for_test`
  - `admit_authority_query_delegate`
  - the `ResumeCursor` runtime methods
  - `aggregate_authorization_scope_bounds`
  - the sync-autopsy `summarize_*` functions
  - `PendingSubscriberControlResponse::message`
- **node_runtime.rs.** These are runtime-feature or not shipped:
  - `declare_upload_root`
  - `enable_authoritative_scalar_exit_refresh`
  - the non-shipped `accept_*` variants
- **mutations.rs.** These are not reached from the browser coverage set, or are not-wasm:
  - readers: `read_value_range`, `read_text_utf16_range`, `read_json_pointer`
  - cell edits: `append` / `splice` / `edit_value_cell`
  - `authorized_physical_cell`
  - `write_staged_large_value_update`
  - the non-`at_ms` `update_with_large_value_mutations`
  - `owned_byte_range`
  - the not-wasm streaming writers `insert_streaming_value*` and `write_streaming_value_with_id`

## Stubs

- **`COVERED_TRACE`** is mapped to `jazz_types__debug_env__covered_input_trace()`, an extern from
  jazz_types. There are no other stubs.
- **Calls into other units** go through extern declarations in `_ext_db-a.h`. These are
  declarations only, so no bodies were invented. Examples are row_availability's
  `receive_current_rows` and `pump_current_rows`, read_barrier's receiver/sender, db.rs's
  `admitted_request_policy_binding`, `queue_local_acknowledgements`, the shape-registration
  validators, and the authorization-scope registry.
- **The mirror types are this unit's view of other units' layouts.** They exist so that this
  unit compiles; the owning units may lay them out differently. Examples are `CoverageGroup`,
  `SubscriberShapeRegistration`, `PermissionAdviceAction`, `ViewUpdatePayload` and the query
  predicate tree.

## Notable mappings

- **Async becomes synchronous.**
  - `async fn` / `.await` become plain calls.
  - `LocalFuture` / `block_on` are a poll-based struct where a future must be stored, as in
    the write-wait state machine and permission advice.
  - Nested `Box::pin(async { .. })` stop scopes become static functions returning `jz_err`
    with a `bool *stop` or flow-code out parameter. In the RegisterShape and Subscribe arms,
    Rust's `return Ok(true)` only continues the receive loop, and the C keeps that
    distinction.
- **`Rc<RefCell<..>>` / `LocalMutex`.**
  - These become raw pointers. `borrow()` / `lock().await` become
    `jazz_db__db__LocalMutexBorrow__borrow` or `jazz_db_node_lock` plus `jazz_db_node_unlock`.
  - Scoped session claims map to `scoped_active_session_claims` and `end_scoped_claims`.
  - Weak subscription refs use `jazz_db_rc_downgrade`, `jazz_db_weak_upgrade` and
    `jazz_db_weak_release`.
- **`dyn Transport`** becomes a vtable struct. A NULL slot runs the trait's default body:
  the default `try_send` clones the message and hands it back on backpressure, and the
  default `try_recv_owned_result` wraps `try_recv_result` as an unleased message.
- **Errors and results.**
  - `Result<T, Error>` becomes a returned `jz_err` plus out-params, and `?` becomes `JZ_TRY`.
  - Node errors cross into db errors through `jazz_node__node__api_error__Error__From__from`.
  - Error variants that are matched on, such as `QueryCapability`,
    `InvalidAuthoritySourceClosure` and `Backpressure`, are code comparisons.
- **Collections and options.**
  - `BTreeMap` / `BTreeSet` / `VecDeque` / `Vec` use the one type-erased `jz_btree`,
    `jz_deque` and `JZ_VEC` implementations from `rt/`. Each Rust map instantiation is a
    `jz_btree_desc` constant instead of a monomorphised copy.
  - Claims maps (`BTreeMap<String, Value>`) are cloned with a string key-clone plus
    `groove__records__Value__clone`.
  - `Option<T>` is a `has_x` flag plus the value.
- **Refresh control flow.** `refresh_detached` returns an int code: fallthrough, retain,
  drop, pushed or error. This replaces the Rust `continue` / retain-push control flow in the
  subscription refresh loop.

## Blockers

None. The compile checks pass and nothing is left unported in the shipped surface.
