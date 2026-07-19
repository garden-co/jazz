# Browser Suite Triage Notes

Date: 2026-07-19
Branch: `codex/jazz-core-engine-swap`

## Baseline

- Repro command: `pnpm --filter jazz-tools test:browser`
- Baseline from `/tmp/browser-local.log`: 45 failed, 136 passed, 2 skipped / 183 tests.
- Failing files at baseline:
  - `tests/browser/alpha-public-flow-gate.test.ts`
  - `tests/browser/db.all.test.ts`
  - `tests/browser/db.subscribeAll.sort.test.ts`
  - `tests/browser/db.subscribeAll.test.ts`
  - `tests/browser/db.transaction-reads.test.ts`
  - `tests/browser/useAll.test.tsx`
  - `tests/browser/useAllSuspense.test.tsx`
- Recent browser-test history:
  - `342c2583b Remove stale transaction wait comments`
  - `9c91f5de2 Refresh async channel subscriptions after writes`
  - `cd3709ba0 Fix invite flow and relation subscription coverage`
  - `e6b87ebc3 Complete native updatedAt write semantics`
  - `f02c54e52 Make subscription payloads relation-capable`

## Fixed Families

| Family                                       | Evidence                                                                                                                                                                                                                                                                                                        | Fix                                                                                                                                                                                                                                                                                               | Verification                                                                                                                                                                                             |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Flat native browser query/subscription drift | Sort, limit/offset, array membership, and flat subscription delivery failures in `db.all`, `db.subscribeAll`, `useAll`, and `useAllSuspense`. Sparse native subscription chunks do not carry public row indexes for flat queries.                                                                               | Flat non-relation subscriptions now refresh from a full local snapshot before publishing deltas; root `offset` is encoded; array-column scalar `in` lowers to `Any(Contains(...))`.                                                                                                               | `pnpm --filter jazz-tools test:browser tests/browser/db.subscribeAll.sort.test.ts` passed. Flat query failures in full run are gone except one isolated timing/full-suite interaction that passes alone. |
| Browser transaction staging/proxy rot        | `db.transaction-reads.test.ts` had 14 failures: read-only commits manufactured empty native writes, browser proxy leaked native required-column wording, rollback raced queued writes, mergeable read-only global wait hung, and browser exclusive tx conflict detection was lost by deferred WASM tx creation. | Read-only commits are no-ops, policy-free browser exclusive txs can stage without identity, proxy setup errors normalize public wording, rolled-back queued writes are discarded, read-only waits short-circuit after commit, and exclusive writes record visible-parent baselines before commit. | `pnpm --filter jazz-tools test:browser tests/browser/db.transaction-reads.test.ts` passed: 27/27.                                                                                                        |

Commits:

- `9de3a988f Refresh flat browser query subscriptions`
- `0ac655cc4 Repair browser transaction staging`

## Remaining Failures

Post-fix full browser run:

- Command: `pnpm --filter jazz-tools test:browser`
- Result: 15 failed, 166 passed, 2 skipped / 183 tests; 6 failed files, 7 passed, 1 skipped.

| Remaining family                    | Tests affected                                                                                | Evidence / decision                                                                                                                                                                                                                                                                                                                                                            |
| ----------------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Websocket convergence updates       | 2 tests in `alpha-public-flow-gate.test.ts`                                                   | Original first signature remains: reader sees the inserted row but not the later update (`done:false` never converges to `done:true`). Writer update waits at `edge`; reader local query stays stale. I tried subscription/local coverage lifetime as a hypothesis and reverted it when it did not change the focused failure. Needs deeper transport/core-sync investigation. |
| Native relation query lowering gaps | `db.all` multi-hop/gather; `db.subscribeAll` hop/gather; `useAll`/`useAllSuspense` hop/gather | Core errors are explicit: `object relation literals are not supported by query predicates`, `relation query joins must connect directly to the output scope in this slice`, and `union/gather/distinct relation query lowering is not unified yet`. These are behavior gaps, not harness weakening candidates.                                                                 |
| Include materialization mismatch    | `db.all.test.ts` include relations                                                            | `todosViaOwner` is decoded as a `Uint8Array` of bytes (`users...`) instead of an included row array. This looks like relation-snapshot/include materialization output-shape drift.                                                                                                                                                                                             |
| Full-suite sort timing artifact     | `db.subscribeAll.sort.test.ts` one test in full run                                           | `keeps order stable when updating a non-sort field` failed in the full run with `expected initial sorted snapshot`, but passes in isolation with the same browser command. Treat as secondary until deterministic repro is found.                                                                                                                                              |

## Gate Notes

- Focused browser checks run:
  - `pnpm --filter jazz-tools test:browser tests/browser/db.subscribeAll.sort.test.ts`: pass.
  - `pnpm --filter jazz-tools test:browser tests/browser/db.transaction-reads.test.ts`: pass.
  - `pnpm --filter jazz-tools test:browser tests/browser/db.all.test.ts -t contains`: pass.
  - `pnpm --filter jazz-tools test:browser tests/browser/alpha-public-flow-gate.test.ts -t "opens public createDb with websocket server config"`: fail, same convergence signature.
- Full browser check run:
  - `pnpm --filter jazz-tools test:browser`: fail, 15/183 failing.
- Other requested gates:
  - `dev/gates/ts-wire-codec.sh`: pass.
  - `dev/gates/no-sensitive-data.sh`: pass.
  - `cargo test -p jazz-tools --features test -j 2`: pass.
  - `pnpm test --filter=jazz-tools --force`: fail in non-browser surfaces under local Node 20. Primary signatures: `node:sqlite` missing, wasm init `fetch failed` / `not implemented... yet...`, and `browser WebSocket is not available`.
- Environment caveat: local shell uses Node `v20.13.1`; package metadata wants `>=22.12`. The suite runs but pnpm prints engine warnings.

Tooling-friction: a browser-suite failure reporter that groups by root error text and records server/transport frame counts per test would have saved most of the websocket wall-clock.

## Flake watch (2026-07-19 ~11:15)

`napi.integration.test.ts > serves policy graph holder queries after importing
data and reopening the local server route` failed once in a full turbo run,
passes isolated and on rerun — suspected port/db-path contention with the new
close/reopen path under suite parallelism. Watch on CI; if it recurs, serialize
that file or isolate its server fixture.
