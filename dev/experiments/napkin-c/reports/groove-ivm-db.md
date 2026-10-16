# groove-ivm-db: napkin C port report

Scope:
- `crates/groove/src/ivm/{mod,graph,op_types,template,execution_layout,activation,routes}.rs`
- `ivm/graph/join_chains.rs`
- `ivm/template/{arguments,sources}.rs`
- all of `crates/groove/src/db/`

Out of scope: `ivm/planner.rs` and `ivm/runtime/` (owned by the groove-ivm-rt-a/b units).

Every in-scope file is ported. All 18 `.c` files pass `build/check.sh` together:
- 9 under `src/groove/ivm/`
- 9 under `src/groove/db/`

Each file was compile-checked on its own as it was finished, and the full set was checked again at the end with no errors.

## Files: Rust non-test LOC vs C LOC

The Rust figures come from `build/rust-loc.txt` (non-test code lines). The C figures are `wc -l`.

| Rust file | Rust LOC | C file(s) | C LOC |
|---|---:|---|---:|
| ivm/mod.rs | 17 | (re-exports only, nothing to port) | 0 |
| ivm/op_types.rs | 659 | ivm/op_types.c + .h | 2364 + 415 |
| ivm/graph.rs | 2334 | ivm/graph.c + .h | 2783 + 388 |
| ivm/graph/join_chains.rs | 196 | ivm/graph/join_chains.c | 260 |
| ivm/template.rs | 241 | ivm/template.c + .h | 312 + 112 |
| ivm/template/arguments.rs | 125 | ivm/template/arguments.c | 189 |
| ivm/template/sources.rs | 132 | ivm/template/sources.c | 195 |
| ivm/execution_layout.rs | 166 | ivm/execution_layout.c + .h | 209 + 43 |
| ivm/activation.rs | 160 | ivm/activation.c + .h | 266 + 37 |
| ivm/routes.rs | 134 | ivm/routes.c + .h | 163 + 46 |
| db/mod.rs | 1751 | db/mod.c + db/mod.h | 1323 + 766 |
| db/batch.rs | 307 | db/batch.c | 272 |
| db/commit.rs | 771 | db/commit.c | 680 |
| db/encoding.rs | 845 | db/encoding.c | 620 |
| db/facade.rs | 1800 | db/facade.c | 1668 |
| db/primary_storage.rs | 581 | db/primary_storage.c | 569 |
| db/query.rs | 712 | db/query.c | 526 |
| db/schema_admission.rs | 511 | db/schema_admission.c | 450 |
| db/storage_helpers.rs | 945 | db/storage_helpers.c | 646 |
| (mirror header) | - | _ext_groove-ivm-db.h | 716 |
| **Total** | **12387** | `.c` 13495 + headers 2523 | **16018** |

Why some C files are larger than their Rust:
- **op_types.c (659 to 2364).** Rust `#[derive(Clone, PartialEq, Eq, Hash, Ord, Debug)]` on the operator and expression enums has to be spelled out in C, one case per variant, as clone, eq, hash, cmp, drop and debug functions.
- **query.c and storage_helpers.c (smaller).** These are mostly thin `async` wrappers and builder chains, which collapse in C.

## Skipped, with reasons

- **`ivm/planner.rs`.** Excluded by the assignment. `plan_query` and `plan_prepared_shape` are declared in the mirror header.
- **`ivm/mod.rs`.** Module declarations and re-exports only.
- **Test-only code:**
  - `db/mod.rs` `mod tests`
  - `batch.rs` `#[cfg(test)]` items (`StagedDatabaseBatch`, `pending_writes_from_operations`)
  - `facade.rs`: `live_attaches`, `open_staged_batch`, and the `#[cfg(feature = "test")]` item `large_value_metadata_entries_for_compatibility`
  - `commit.rs` `#[cfg(test)] commit_batch`
  - the `#[cfg(feature = "cold-settle-attribution")]` blocks
- **IVM helpers left out:**
  - `IvmGraph::node_mut`: test-only accessor.
  - `legacy_binding_shape`: test-only.
  - `IvmGraph: Clone`: no browser rows; the runtime clones through its own snapshot.
- **Rustdoc examples.** Doc-tests only; nothing to port.

## Synchronous-port decisions (async to sync)

**Locks**
- **`large_value_lifecycle.lock().await`.** `LifecycleMutex` lock/unlock wraps a `*_locked` helper, so every early return releases the lock. Contention panics, which cannot happen single-threaded unless a lock is re-entered.
- **`commit::lock_large_value_lifecycle`.** Loops on `try_lock` and `poll_progress`, and panics if no progress is pending. This is the case where an IVM chunk install owns the lifecycle mutex.

**Persistence and progress loops**
- **`AppliedBatch::persist`.** Registers its waiter, runs `jz_task_run_ready()`, and panics if that makes no progress.
- **`persist_with_progress`.** Polls database progress until it is this publication's turn, then persists.
- **`next_*` subscription and multisink functions, `drive_progress`, `prepare_for_storage_extraction`.** Each loops with a flag waker plus `jz_task_run_ready()`, and panics if nothing wakes it.
- **`cooperative_yield_once`** becomes a single `jz_task_run_ready()`.
- **`write_direct_records_with_progress`.** Rust drives `poll_progress` only while the write future is Pending. The synchronous `write_many` is Ready on its first poll, so that branch is never entered and the wake bridge is not built.

**Rust std types**
- **`Weak<T>`.** Becomes a strong-count check on the jz_rc payload (`groove__db__weak_upgrade`).
- **`HashSet` / `HashMap`.** `jz_hmap`, as in `schema_admission` and `compute_table_deltas`.
- **`BTreeSet` / `BTreeMap`.** `jz_btree`. A one-shot `collect::<BTreeSet<_>>()` becomes sort plus dedup.
- **`apply_batch`'s borrowed exact-key `HashMap`.** Replaced by direct `BTreeMap` lookup. Same result, one fewer container.
- **`serde_json::Value::pointer`.** Implemented as a small RFC 6901 walker in `facade.c` over `jz_json`, including `~0`/`~1` unescaping and the array-index rules.

**Errors**
- **db error codes** start at 0x0B01. The Display strings are unchanged.
- **Transparent variants** pass the inner `jz_err` through unchanged: IvmRuntime, Storage, RecordEncoding, SubscriptionFailed and QueryPlanning.
- **`is_retryable_upload_error` and the `ReachabilityError` match.** These test the inner error code ranges:
  - storage 0x04xx
  - large_values 0x11xx
  - chunks 0x12xx, including Backend, Unavailable, Retryable and Integrity

## Mirror-header notes for the coordinator

**`_ext_groove-ivm-db.h`** declares what this unit uses from records, schema, storage, large_values, chunks and ivm::runtime. The layouts follow those units' headers where they existed when they were read. Known differences to reconcile at link time:
- **`StagedWriteOverlay__new_owned`.** Mirrored as `(base_rc, base_dyn, state)`. The storage unit's file takes `(base, base_rc, staged)`.
- **`groove_StrVec` (`JZ_VEC(jz_str)`).** Now declared once in the mirror header, and `ivm/op_types.h` uses it. It is used for `column_family_names`, `IndexSchema.columns`, `TableVariant.fields` and `output_key_fields`, so the function-pointer types unify.
- **`BoxedStorage__drop`.** Added to the mirror; it matches `src/groove/storage/mod.c`.
- **`IvmRuntime__index_ids` returns a borrowed handle.** This unit calls `jz_rc_clone` wherever Rust does `Rc::clone`.

**Ownership convention used in this unit:**
- `new_owned` consumes the rc refs passed to it; `StagedWriteOverlay__new` borrows.
- A by-value struct argument (`LargeValueRef`, `StagedChunkVec`, `TableSchema`, `IndexSchema`, `DatabaseSchema`) is consumed on every path, including errors.

**`mod.h`** carries the db-internal prototypes. They were generated from the `.c` files, then corrected to declarations only.

## Remaining / blocking

None. Every in-scope Rust file has a C counterpart that compiles.
