## TS adapter attribution

Context: worker-side subscription delivery in `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts`.

Micro-harness: `JAZZ_TS_ADAPTER_PERF=1 pnpm --dir packages/jazz-tools exec vitest run --config vitest.config.ts src/runtime/native-runtime/runtime.test.ts -t "TS adapter perf canary" --reporter=verbose --silent=false`

| Run       | 24,000-row reset | 95 small reset chunks | Small median | Notes                                                                                                     |
| --------- | ---------------: | --------------------: | -----------: | --------------------------------------------------------------------------------------------------------- |
| After fix |           6.51ms |          1.34ms total |      0.008ms | Fake native source, one text column, reset delivery through `createSubscription` + `executeSubscription`. |

Attribution from the adapter pass:

| Area                         | Attribution                                                                                                                                                                                                                                                      | Fix                                                                                                                                                                                                                                         |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `createSubscription` prepare | The plain-query path prepared the query for `subscribe`, then called `prepareQuery(queryJson)` again while constructing `SubscriptionState`. The second call hit the prepared-query cache but still re-encoded the query JSON and rebuilt the byte key.          | Reuse the prepared query object from the subscribe call when storing subscription state.                                                                                                                                                    |
| Row decode fixed work        | `rowsFromBatches` rebuilt `nativeRowFieldPlans` on every batch decode. For many small chunks this repeated schema column maps, descriptor scans, public/internal field classification, and magic/provenance checks even when the table descriptor was identical. | Cache native row field plans per schema and descriptor key.                                                                                                                                                                                 |
| Row decode per-row work      | The remaining 24k reset cost is row proportional: record value decoding, UUID formatting, value object construction, and `valuesByColumn` creation.                                                                                                              | Left unchanged in this pass because it is semantic payload construction; the measured fake-source cost is low after fixed-work removal.                                                                                                     |
| Frame build                  | Plain reset delivery used to decode values into `RowState` and then encode those values back into the native row frame.                                                                                                                                          | Large plain reset chunks now build the outgoing frame from native row batches, preserving raw record bytes when the descriptor already matches the public output columns and otherwise repacking raw field bytes without JS `Value` decode. |
| Edge refresh                 | The existing `this.serverTransport && subscription.opened && !chunk.reset` guard remains unchanged.                                                                                                                                                              | No change.                                                                                                                                                                                                                                  |

Before-fix browser profile motivating this lane: cold profile had 99 `subscription_apply_chunk` spans totaling 6.92s, with the 24,045-row chunk at 2.01s and 95 under-100-row chunks at 4.64s total; warm profile had a 5.44s `createExecutedSubscription` request with ~0.9s wasm-core spans and ~2.45s unattributed adapter time.

The kept harness is skipped by default because timing is machine-sensitive. It is useful as a local receipt for adapter-side constant cost, not as a normal correctness gate.

## TS adapter reset pass-through

Evidence from the worker subscription pipeline:

| Consumer                 | Code path                                                                                                               | Reset-chunk need                                                                                                                                                                                                                 |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Envelope read            | `normalizeSubscriptionChunk` calls `readNativeSubscriptionDelta(new PostcardReader(...))` for delta chunks.             | Needs table names, descriptors, row id bytes, and raw record bytes to build the outgoing native frame. It does not need decoded values.                                                                                          |
| Subscription state       | Plain reset chunks previously assigned `subscription.rows` after `rowsFromBatches` or `refreshPlainSubscriptionRows`.   | Needs ordered state only if a later worker-side incremental delta must compare/apply against prior rows. The final implementation stores packed reset batches and materializes them lazily before non-reset apply/refresh paths. |
| Visible-row replay       | `executeSubscription` replays `visibleRows` as a reset if already opened.                                               | For packed resets it can replay the saved `NativeRowDelta` directly; no values required.                                                                                                                                         |
| Settled coalescing       | `publishSubscriptionRows` defers unsettled global callbacks and later publishes a reset or diff.                        | For a deferred reset it can retain and publish the packed reset frame. Diff publication still materializes rows when needed.                                                                                                     |
| Snapshot refresh         | `snapshotRefresh` normally refreshes plain subscription rows through `db.all*`.                                         | Reset chunks do not need edge refresh, and the reset payload is already the authoritative subscription snapshot. The packed path is restricted to plain reset chunks with no relation payload and identity projection.           |
| Edge refresh             | Guarded by `this.serverTransport && subscription.opened && !chunk.reset`.                                               | Reset chunks need nothing; the guard is unchanged.                                                                                                                                                                               |
| Relation materialization | Relation snapshots/deltas and array subqueries call `materializeRelationRows`.                                          | Needs full row values, so relation paths are excluded from the packed reset fast path.                                                                                                                                           |
| Output-column mapping    | `subscriptionOutputColumns` always returns root columns for plain table queries, including the no-explicit-select case. | The fast path now detects identity projection by comparing `outputColumns.rootColumns` to the table's full public column list. Non-identity projections still use the legacy path.                                               |

Pass-through scope:

| Shape                                                                      | Behavior                                                                                                                                                                          |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Plain reset, no relation payload, no array subqueries, identity projection | Store packed batches in subscription state, build `NativeRowDelta.added` from row id bytes and raw public record bytes, and replay that frame while visible state remains packed. |
| Later non-reset after a packed reset                                       | Lazily decode the packed reset batches into `RowState[]` before applying the incremental delta.                                                                                   |
| Relation/include/non-identity projected subscriptions                      | Keep the existing decode/materialize/re-encode path.                                                                                                                              |

Guard bug found after real-app measurement:

| Bug                                                                                                                                                                                                                                                                                 | Fix                                                                                                                                                                              |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `plainResetChunkCanStayPacked` required `subscription.outputColumns === null`, but `subscriptionOutputColumns` always builds a `SubscriptionOutputColumns` for plain table queries. No-explicit-select table subscriptions therefore never used the packed path in the real worker. | Treat identity projection as fast-path eligible: `outputColumns.rootColumns` must equal the table's full public column list after filtering internal and hidden include columns. |

Main-thread decode/filtering decision:

| Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Decision                                                                                                                                                                                                                                                                                                                                                                                  |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `persistent-browser-runtime.ts` reconstructs `NativeRowDelta` from transferred buffers only; it does not carry native row descriptors. `SubscriptionManager.handleDelta` calls `decodeNativeDelta(delta, nativeColumns)`, and `decodeNativeDelta` decodes each raw row with `decodeNativeRow(id, columns, data)`. That means the main thread interprets frame raw bytes using the app/output column descriptor, not the original native batch descriptor. | Filtering must happen before the frame crosses to the main thread. The worker packed-frame builder now repacks batches whose descriptor includes internal fields such as `tx_time`, slicing raw field bytes and creating a public-column record without constructing JS `Value` objects. If the native descriptor exactly matches the public columns, raw records pass through unchanged. |

Equality regression:

| Test                                                                                                                 | Purpose                                                                                                                                                                                             |
| -------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `delivers packed reset rows with the same public shape as legacy decode when native batches include internal fields` | Asserts a packed reset chunk containing internal `tx_time` decodes on the main-thread path to the same public row object shape: only `id` and public `values`, with no internal/meta field leakage. |

Slice canary receipt:

| Run                    | 24,000-row reset read/apply/frame | 95 small reset chunks | Small median | Notes                                                                                                                                                                                                  |
| ---------------------- | --------------------------------: | --------------------: | -----------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Before corrected guard |                            7.13ms |          1.42ms total |      0.008ms | This receipt was invalid as a pass-through proof: the fake `db.all()` returned an empty snapshot, so the old dead guard was not caught by row-count assertions.                                        |
| After corrected guard  |                           24.84ms |          1.78ms total |      0.013ms | Canary now asserts delivered `addedCount` equals source row count. The 24k reset path is really engaged and includes envelope parse, public-frame build, and worker-style `transferableBuffer` checks. |

End-to-end real-app verification note: `/Users/anselm/jazz-private/workspaces/boredm` serves the worker from the main checkout through symlinks. The orchestrator runs that post-merge; this lane records the local adapter gates only.

## Packed reset real-app guard trace

Trace setup: temporarily emitted worker spans from `plainResetChunkCanStayPacked` and the packed reset frame builder, rebuilt `packages/jazz-tools` dist in this worktree, relinked the boredm app's `node_modules/jazz-tools` and `node_modules/jazz-wasm` to this worktree, and restarted `/Users/anselm/jazz-private/workspaces/boredm` with `JAZZ_CORE_ROOT=/Users/anselm/jazz_core-ts-adapter scripts/demo-stack.sh --skip-build --prod`.

Important setup correction: `demo-stack.sh` defaults `ENGINE_ROOT` to `/Users/anselm/Documents/jazz_core`, and the boredm `node_modules` symlinks also pointed at that main checkout. Early trace runs were therefore measuring the main checkout bundle, not this worktree. The final verification used the worktree links above; `jazz-napi` remained linked to the built main package because this slice only changes `jazz-tools` TypeScript.

Empirical guard finding for the 24,045-row `dropdown_entry` reset chunk:

| Guard conjunct                 | Trace value                                                                            |
| ------------------------------ | -------------------------------------------------------------------------------------- |
| `reset`                        | `true`                                                                                 |
| `delta.updated.length === 0`   | `true`                                                                                 |
| `delta.removed.length === 0`   | `true`                                                                                 |
| `!relationSnapshot`            | `true`                                                                                 |
| `arraySubqueries.length === 0` | `true`                                                                                 |
| identity projection            | `true` (`outputColumnsLength = 11`, `tableColumnsLength = 11`, `firstMismatch = null`) |
| old `!relationDelta`           | `false`                                                                                |

The chunk carried a `relationDelta`, but the adapter only consumes relation deltas in `applyRelationSubscriptionDelta` when `subscription.relationMaterialization.arraySubqueries.length > 0`. For a plain table reset with no array subqueries, that relation delta is irrelevant to worker-side bookkeeping, and routing to `snapshotRefresh` forced the 24k row decode. The guard now allows `relationDelta` only in that no-array-subquery case.

Second trace finding after enabling that guard: the fast path engaged, but the main thread threw `unexpected end of record` / `invalid offset`. Descriptor trace showed the engine batch descriptor uses `user_*` fields with outer optional wrappers:

- output `dropdowns_id: Uuid`, source `user_dropdowns_id: Optional<Uuid>`
- output `options: Array<Text>`, source `user_options: Optional<Array<Text>>`
- output nullable columns such as `allow_custom: Optional<Boolean>`, source `user_allow_custom: Optional<Optional<Boolean>>`

`createRecordValueDecoder` unwraps source optional layers. The packed frame builder therefore had to re-wrap decoded bytes when the output column is nullable before creating the public output record. The equality test covering this is `rewraps user field option bytes when packed reset frames filter engine records`.

Real-app receipts:

| Run                                    | Harness dir                                                        | Warm apply result                                                                                                                                    |
| -------------------------------------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Before fix, worktree-linked with trace | `/Users/anselm/boredm-harness-v2/browser-profile-20260721T212100Z` | `subscription_apply_chunk` max `2048.1ms`, 24,045 rows; guard declined on `relationDelta`, branch `snapshot-refresh`.                                |
| Guard-only fix, before optional rewrap | `/Users/anselm/boredm-harness-v2/browser-profile-20260721T212457Z` | Worker apply max `100ms`, but page failed decoding because packed frame bytes did not match output column nullability.                               |
| Final clean fix                        | `/Users/anselm/boredm-harness-v2/browser-profile-20260721T213401Z` | Warm `subscription_apply_chunk` total `120.6ms`, max `98.8ms`; no apply span over 1000 rows, so the 24,045-row reset stayed packed. UI guard passed. |

Temporary instrumentation was removed before the final clean run.
