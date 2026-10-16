# groove-ivm-rt-a

The IVM runtime core of groove: the subscription registry, the tick loop, the tick evaluator, and record projection.

## Rust files covered

| Rust file (crates/groove/src/ivm/runtime/) | Rust lines (non-test) | C file(s) | C lines |
|---|---|---|---|
| subscriptions.rs | 5454 (tests 5455-5508) | subscriptions.c + .h | 3085 + 365 |
| runtime_tick.rs | 4287 (tests 4278-4505 excluded) | runtime_tick.c + .h | 3756 + 78 |
| evaluator.rs | ~3690 (tests 193-381 and cfg(test) counters excluded) | evaluator.c + .h | 2937 + 266 |
| evaluator/kernels.rs | 400 | evaluator/kernels.c | 265 |
| evaluator/lookup.rs | 323 | evaluator/lookup.c | 353 |
| record_projection.rs | ~2270 (cfg(test) thread_local excluded) | record_projection.c + .h | 2137 + 75 |
| (mirror declarations) | - | _ext_groove-ivm-rt-a.h | 1034 |

The totals are about 16.4k Rust non-test lines and about 14.4k C lines, headers included.

`build/check.sh` compiles all six .c files cleanly, with no errors or warnings.

## Skipped and why

- **Test-only code:**
  - all `#[cfg(test)]` modules;
  - the subgraph-walk and async-frame counters in evaluator.rs;
  - `active_input_source_state_count` in subscriptions.rs;
  - `evict_eval_memo_for_tests` in runtime_tick.rs.
- **Feature-gated or diagnostic code:**
  - `cold-settle-attribution` tracing spans;
  - the `GROOVE_TRACE_INDEX_BY` environment trace in kernels.rs.
- **Debug-only checks.** These `debug_assert`s are omitted:
  - the `pending_incremental_polling` re-entrancy assert;
  - `affected_recursive_nodes_are_current`.
- **One-sink `Subscription` wrappers in subscriptions.rs:** `subscribe_one_sink*`, `prepare_one_sink*`, `bind_shape_one_sink*`, `single_sink_subscription`, and `extract_sink_deltas`. These are a convenience layer over the multisink path. The browser client uses the multisink path, and these functions are absent from the browser-coverage CSV.
- **Also absent from the CSV, or dead code:**
  - the auto-direct planning chain;
  - `propagate_binding_through_frontier` (dead code);
  - `record_store_for_table`.

## Stubs and invented extern names

No real logic is stubbed. Every other non-test function is translated branch by branch, with the same error codes and messages.

**Invented extern names.** The Rust names some calls without a free function, for example a derived `Clone` or a `Default`. For these I named an extern by the naming scheme. The coordinator must check that the owning unit provides each one:
- `EvaluationRequestKey__clone`
- `AsOfArrangement__clone`
- `StagedWriteState__default`
- `OwnedChunkProvider__with_install_observer`
- `NodeRuntimeMeta_drop` and `NodeRuntimeMeta_clone`
- `RouteTable__barriers`
- `EvaluationMemo__for_layout`

**Guessed mirror layouts.** I guessed these struct layouts in `_ext_groove-ivm-rt-a.h` from the Rust fields:
- `ExecutionLayout`
- `ActivationPlan`
- `EvaluationRequestKey`
- `EvaluationRequests`
- `EvaluationInputs`
- `RouteTable`

## How Rust mapped to C

- **Async becomes synchronous polling.**
  - Each `Poll` returns a u32: 0 means ready and `GROOVE_POLL_PENDING` (0x1fff) means pending.
  - Each `.await` on a real suspension, and each `poll_fn` loop, is a poll loop with `jz_task_run_ready()` between polls.
  - The persist flush completes synchronously, so the flush future (`persist_flush`) never stays pending.
- **Shared state.**
  - `Rc<RefCell<PendingIncrementalState>>` is a pointer owned by the runtime and allocated lazily. Each borrow follows "take the state out, work on it, put it back", which is the `mem::take` the Rust code does.
  - `Rc<Cell<..>>` and `Rc<RefCell<..>>` fields of `PendingResidentPublication` are `jz_rc` payloads.
  - Each `Arc<HashSet<NodeId>>` is a `jz_rc` hashmap with `make_mut` semantics.
- **Wakers.** The `Arc<impl Wake>` owner fan-out waker is a heap context. It is leaked on purpose because the waker can outlive the poll. This leak is a small, known difference from Rust.
- **Generics.**
  - Each `S: OrderedKvStorage` function is instantiated once, over `OrderedKvStorage*` (a dyn object).
  - Rust instantiates `tick`, `hydration_snapshot*` and the `flush_pending_binding_retractions` family once per storage type. C has one copy of each.
  - `OwnedStorage<'a>` is an rc handle.
- **Enums.** `PendingEvaluation`, `EvaluationEntry`, `EvaluationFailureKind`, `DetachOn` and `HydrationMode` became small tagged structs or enums.
- **Moves.** A Rust move is a by-value struct copy, and the source container is re-initialised. Maps that mirror runtime maps reuse the runtime map's own descriptor (`self->operator_states.d` and so on), so the two hash and compare consistently.
- **Big async functions become flat C.**
  - `IncrementalEvaluation::poll`, `EvaluationSession::poll`, `poll_incremental` and `begin_tick_with_params_and_notification_policy` are the largest functions in the unit.
  - Each one became flat C with `goto` cleanup paths. In Rust they compile into large generator state machines; in C there is no state machine at all.
  - So the C for runtime_tick.rs is probably smaller relative to its Rust WASM than the line counts suggest.
- **CSV.** The tick, hydration and pending-evaluation functions of runtime_tick.rs appear in browser-coverage.csv, so they ship in the browser build and are ported in full.
