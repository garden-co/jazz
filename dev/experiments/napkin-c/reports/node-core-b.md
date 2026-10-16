# node-core-b: napkin C port report

Unit: `jazz_node` (crates/jazz/layers/node/src). It covers lib.rs, peer.rs, all of peer/, and these node/ files: mod, views, open_tx, policy, currency, oracle, catalogue_ingest, source_resolution, descriptor_roles, exclusive_read_evidence, recovery, terminal_record, merge_ops, migration_validation, global_state, eviction, row_availability, terminal_root, layer_stack, supporting_frontier, node_aliases, api_error, relay_scope, database_slot and refs.

Status: **every in-scope Rust file that ships in the browser build is ported and compiles.** The check script runs `build/check.sh` over all unit files and gives `OK (30 files)`. The peer files also compile cleanly with `-Wall`.

I also compiled all 71 `src/jazz_node/**/*.c` files to objects and ran `llvm-nm`. No global symbol is defined twice. The duplicates the coordinator reported (`jzn_desc_str_set` / `jzn_desc_u64_set`) are fixed: `mod.c` now keeps them as `static` copies (`mod_desc_str_set` / `mod_desc_u64_set`), and `physical.c` owns the extern definitions.

## Files

Rust line counts are non-test (from `build/rust-loc.txt`). C paths are relative to `src/jazz_node/`.

| Rust file | Rust lines | C file | C lines |
|---|---:|---|---:|
| lib.rs | 22 | lib.c | 9 |
| peer.rs | 122 | peer.c + peer/peer.h | 102 + 199 |
| peer/subscription_state.rs | 235 | peer/subscription_state.c | 303 |
| peer/delivery.rs | 185 | peer/delivery.c | 245 |
| peer/publication.rs | 2530 | peer/publication.c | 1970 |
| peer/known_state.rs | 166 | peer/known_state.c | 174 |
| peer/repair.rs | 100 | peer/repair.c | 109 |
| node/mod.rs | 2419 | node/mod.c + node/mod.h | 1756 + 749 |
| node/views.rs | 2591 | node/views.c | 3057 |
| node/open_tx.rs | 2303 | node/open_tx.c | 2444 |
| node/policy.rs | 1289 | node/policy.c | 1424 |
| node/currency.rs | 1231 | node/currency.c | 1286 |
| node/catalogue_ingest.rs | 796 | node/catalogue_ingest.c | 858 |
| node/source_resolution.rs | 659 | node/source_resolution.c | 788 |
| node/descriptor_roles.rs | 544 | node/descriptor_roles.c | 620 |
| node/exclusive_read_evidence.rs | 369 | node/exclusive_read_evidence.c | 502 |
| node/recovery.rs | 269 | node/recovery.c | 202 |
| node/terminal_record.rs | 263 | node/terminal_record.c | 304 |
| node/merge_ops.rs | 262 | node/merge_ops.c | 284 |
| node/migration_validation.rs | 244 | node/migration_validation.c | 212 |
| node/global_state.rs | 236 | node/global_state.c | 245 |
| node/row_availability.rs | 190 | node/row_availability.c | 172 |
| node/terminal_root.rs | 169 | node/terminal_root.c | 133 |
| node/layer_stack.rs | 93 | node/layer_stack.c | 119 |
| node/supporting_frontier.rs | 86 | node/supporting_frontier.c | 122 |
| node/node_aliases.rs | 78 | node/node_aliases.c | 80 |
| node/api_error.rs | 76 | node/api_error.c | 96 |
| node/relay_scope.rs | 58 | node/relay_scope.c | 66 |
| node/database_slot.rs | 35 | node/database_slot.c | 30 |
| node/refs.rs | 26 | node/refs.c | 26 |
| (foreign mirrors) | | _ext_node-core-b.h | 1482 |

Totals: 17,646 non-test Rust lines became 18,686 lines of C (.c + .h), plus 1,482 lines of mirror header.

## Skipped (not ported)

- **node/eviction.rs (200 lines):** no rows in `browser-coverage.csv`, so it is not in the browser build.
- **node/oracle.rs (957 lines):** used only by tests and the testing feature, and also has no CSV rows.
- **node/legacy_test_future.rs:** test-only.
- **All `#[cfg(test)]` code**, including:
  - peer/tests.rs (6550 lines);
  - delivery `canonical_set_delta`;
  - the `#[cfg(test)]` `view_update_for_*` helpers in views.rs.
- **`#[cfg(any(test, feature = "testing"))]` code:**
  - `maintained_subscription_count`, `review_publication_count`, `scope_relay_*_for_test`;
  - the `FAIL_NEXT_CLONED_SUBSCRIPTION_RESET` injection.
  - The `rehydrate_attempts` metric field is kept, but it is never incremented.
- **Functions that are dead outside tests or absent from the CSV:**
  - mod.rs `validate_deployment_snapshot`;
  - descriptor_roles `encode_current_payload_record`;
  - policy `authorization_actions_for_versions_in_transaction`;
  - open_tx `tx_write_mergeable` / `tx_patch_mergeable`.
- **views.rs cold-settle-attribution tracing:** diagnostics only.

## Mapping notes

- **Naming:** a Rust path `a::b::C::f` becomes `a__b__C__f`. Peer functions are `jazz_node__peer__PeerState__x` (the `JZP()` macro), which matches the names jazz_db already calls. publication.rs, known_state.rs and repair.rs are `include!`d into peer.rs in Rust. Here they are separate translation units that share `peer/peer.h`.
- **Async becomes sync:**
  - `Option<T>` results use out-pointers, with NULL meaning None.
  - `Poll::Pending` is reported through a `ready` flag (`query_update_for_subscription_with_opts_and_waker`).
  - Wakers are `const jz_waker *`.
- **Guards and Drop:**
  - `scoped_active_session_claims` returns the node, and its Drop becomes an explicit call to the mirror `end_scoped_active_session_claims`.
  - The `AuthorizationSupportPeerScope` Drop (forget the subscription if the proof is incomplete, then restore role and permission identity) becomes the `done:` block of `rehydrate_authorization_support_query_for_identity`.
- **Arc:**
  - `Arc<BTreeMap<String, TableSchema>>` uses `jz_rc`.
  - `Arc<ReadViewSpec>` inside `CachedPeerQueryPlan` is a deep clone.
  - `PreparedQueryPlanHandle` is a `jz_rc` pointer that is released on replace or clear.
- **Collections:**
  - `publication_states` maps to heap `PeerSubscriptionState*` values, so references stay stable across `entry().or_default()`.
  - BTreeSet/BTreeMap become `jz_btree` with explicit descriptors.
- **debug_assertions:** the duplicate-row check in `apply_outgoing_view_delta` is under `#ifndef NDEBUG`.
- **Traces:** the trace lines `JAZZ_COVERED_INPUT_TRACE` and `JAZZ_REHYDRATE_TRACE` keep their keys. Values that need a Rust `Debug` impl not mirrored here (claims maps, authority keys) are printed as counts or `Some`/`None`.
- **NodeState:** there is one `jazz_node_NodeState` definition (node/mod.h). All of this unit's `impl NodeState` blocks across files use it.
- **views.rs:** the `version_bundle_for_exact_storage_*` wrapper is inlined at its single `MBVS_EXACT_STORAGE` call site.
- **Mirror signatures:** these were chosen per unit, so they differ in places:
  - My `VersionRecordVec` holds pointers; protocol.h has it by value.
  - `VersionBundle` and `ResultTransitions` are mirrored independently, in roughly the layout of their owners.
  - core-a names `version_tx_id` under `state__read_payload`.
  - jazz_db calls some peer functions with other argument shapes. For example, it calls `admits_relay_binding(peer, &identity, &claims)`, while mine takes a `PolicyBinding*`, and `client_link(identity)` returns by value there, while mine fills an out-param.

  These are link-level mismatches that a napkin port tolerates. Nothing includes another unit's header.

## Blocking

Nothing.
