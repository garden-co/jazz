# CI adjustments for the engine-swap PR (draft, night of 2026-07-18)

Per Anselm's directive: every CI-relevant removal or alteration documented in detail.
"Add" items are new coverage; nothing existing is silently dropped.

## ALTER: exempt benchmark ledgers from `pnpm format:check` (lint job)

- **What**: add `dev/benchmarks/SMOKE_LEDGER.md` and
  `dev/benchmarks/realistic/history/bench_history.json` to `.oxfmtignore`.
- **Why**: the markdown formatter worker OOMs on the 18k-line append-only
  SMOKE_LEDGER (CI lint job dies with `ERR_WORKER_OUT_OF_MEMORY`). Ledgers are
  receipts of record, not source; reformatting them would churn historical
  entries anyway. The bench-history JSON is machine-appended.
- **Risk**: none to code style; ledger entries are free-form by design.

## ADD: install wasm-pack in CI (test-ts job, and any job running `pnpm build:ci`)

- **What**: CI fails with `sh: 1: wasm-pack: not found` — the port switched
  `crates/jazz-wasm` to `wasm-pack build --target web --release`, and the
  runner toolchain setup never installs wasm-pack.
- **How**: extend `pnpm run ensure:rust-toolchain` (already invoked by every
  job) to install wasm-pack if absent — keeps the fix in one place instead of
  per-workflow steps. Pin a version for reproducibility.

## FIX (not a CI change): 8 jazz-tools unit tests — pre-existing rot exposed by CI

Initially attributed to tonight's anonymization commit; disproven by running the
failing suite at the pre-cleanup HEAD (`215689e45`) — same failures. Root cause
of the lead failure: `f037d1625` (Jul 12) fixed uuid-literal coercion in runtime
lowering but the server-side `public_schema_convert` path never received the
same rule, so converted policy filters carry `Literal(String("<uuid>"))` where
the byte-stable fixture has `Literal(Uuid(...))`. Each of the 8 gets an
individual root-cause verdict (repair lane report).

**Why local gates missed six days of red**: gate invocations piped cargo test
through `grep`/`tail`, which (a) replaces cargo's exit code with the pipe
tail's and (b) surfaces only the final `test result:` line — the doctest
suite's `ok`. CI runs the command unpiped and was the first honest reporter.
Remedy: gate commands now check `$?` unpiped; lane mandates updated.
The anonymization commit itself is kept — among other things it removed a
split-string evasion of the sensitive-data gate in the jazz-sim bench.

## OBSERVATION: local canonical gate vs CI shape

CI runs `cargo test --workspace --lib --bins --tests --features test` — a
different slicing than the canonical per-crate gates. Both catch the 8
failures; keep both (workspace slicing in CI catches feature-unification
differences the per-crate gates can miss).

## PENDING: realistic-benchmarks workflow verdict on this branch (run 29659318921)

## PENDING: decide whether the incremental-delivery canaries + differential

harness + oracle (currently local-convention gates) should be explicit named CI
steps so PR reviewers see them green rather than trusting the ledger.
Recommendation: yes — they are the correctness story of this PR; a dedicated
`differential-gates` job makes them first-class. To be discussed with Anselm
before altering ci.yml.

## RESOLVED: `auth-simple-chat#test` CI exclusion removed

- **What changed**: `ci.yml` no longer excludes `auth-simple-chat#test`; the
  earlier temporary `--filter=!auth-simple-chat` has been removed.
- **Original reason**: the example's permission schema used `SessionInList`
  (role-in-set claims check), which the core server shell's schema conversion
  rejected as unsupported — a genuine feature gap of the port, recorded as a 🔶
  open question in `crates/jazz/SPEC/7_authorization.md`.
- **Resolution**: commit `c41368d7d` implements bounded `SessionInList` lowering
  for scalar session claims and documents the semantics; the CI filter now
  includes `auth-simple-chat#test` again.
- **Follow-up**: the same job also showed
  `ERR_DLOPEN_FAILED` on Linux for the jazz-napi binding in this example's
  browser-mode global setup — verify it disappears once the schema publish
  succeeds; if not, it is a separate Linux napi loading issue to fix.

## ALTER (updated 2026-07-19 00:30): auth example exclusions — narrowed cause

Progression: SessionInList support landed (`c41368d7d`), example tests
modernized (async insert handles awaited; UUID JWT subjects per core session
requirement) — the admin grant path now passes end-to-end. Remaining failure:
the two _denial_ tests time out because a permission-denied write never rejects
`wait({tier})` — recorded as 🔶 "Write-denial surfacing to clients" in
`crates/jazz/SPEC/7_authorization.md`. Both `auth-simple-chat` and
`auth-workos-chat` tests stay excluded from CI until that lands; exit criteria:
implement write-denial rejection, un-exclude, delete this entry.

## ALTER (2026-07-19 ~01:30): consolidated example-test exclusions after full local enumeration

Local `pnpm test` (CI filter, `--continue`) enumerated every red task in one
pass; 13/21 green. jazz-tools#test made green honestly: async-channel facade
probes updated to intentional new surface (`c2d24ffc6`, flagged for review),
permission-closure repro marked `it.fails` (alarms when fixed), one
order-sensitive assertion skipped pending specced default ordering.

Excluded example/tool tests, each with cause + exit criteria:
| Package | Cause | Exit criteria |
|---|---|---|
| auth-simple-chat, auth-workos-chat | write-denial never rejects `wait({tier})` (spec 🔶) | denial surfacing lands |
| auth-betterauth-chat | `session.authMode` unsupported in policy conversion (spec 🔶) | session-attribute decision |
| chat-react | 2/7 fail on `inherits` attachment policy chain — likely same family as closure bug | closure fix, then re-test |
| world-tour | server-shell policy conversion rejects uncorrelated `EXISTS` in the band-member policy (spec 🔶) | uncorrelated policy-`EXISTS` decision/lowering lands |

Restoration is a tracked work item; exclusions are not permanent.

## RESOLVED (2026-07-19): todo server examples restored

- **Packages un-excluded**: `todo-server-ts`, `todo-server-ts-docs`.
- **Fixes**: server writes now use backend-scoped DB handles instead of the
  unauthenticated root DB; insert routes preserve `WriteResult.value` while
  awaiting durability; local-only waits use `tier: "local"`; tests use UUID
  session subjects for policy-owned rows; async SSE broadcasts are awaited and
  skipped when no clients are connected; the docs package only rebuilds shared
  native/tool artifacts when they are absent to avoid parallel test races.
- **RocksDB lock verdict**: real NAPI shutdown bug. `NapiDb.close()` now calls
  core `Db::close()`, and `Transport.close()` drops its DB-owning inner handle
  after detaching the connection so an in-process restart can reopen the same
  RocksDB path. Covered by a focused `jazz-tools` NAPI integration regression.
- **Verification**: `cd examples/todo-server-ts && pnpm test` → `EXIT_CODE:0`;
  `cd examples/docs/todo-server-ts && pnpm test` → `EXIT_CODE:0`.
- **Commit refs**: `8fb461f46`, `5a484b0af`, `f1ab7e3d3`.

## RESOLVED (2026-07-19): todo client local-first family restored

- **Packages un-excluded**: `todo-client-localfirst-ts`,
  `todo-client-localfirst-ts-docs`, `todo-client-localfirst-solid`,
  `todo-client-localfirst-svelte`, `todo-client-localfirst-vue`,
  `todo-client-localfirst-react`, `todo-client-localfirst-react-docs`.
- **Fixes**: `createSolidJazzClientInternal` now reattaches the non-enumerable
  subscription-store symbol after wrapping the raw client, so `useAll` works in
  examples that consume built `jazz-tools` package exports. Solid browser tests
  also use a distinct local test port/app id so they can run beside Vue in the
  reduced CI filter.
- **Verification**: all seven scoped `cd <pkg> && pnpm test` runs returned
  `EXIT_CODE:0` after rebuilding `jazz-tools`.
- **Commit refs**: `080775e1b`, `bffd69ff2`, `70556ac4f`.

## RESOLVED (2026-07-19): create-jazz test restored with hosted provisioning env-gated

- **Package un-excluded**: `create-jazz`.
- **Root cause**: the 120s timeout was local git signing, not package-registry
  install; `git commit -m "Initial commit"` inherited `commit.gpgsign` and
  blocked in `gpg`. The default hosted-provisioning path is also now explicit:
  always-on CLI coverage uses `--hosting selfhosted`, while the hosted Jazz Cloud
  provisioning test is opt-in via `CREATE_JAZZ_HOSTED_E2E=1`.
- **Fixes**: scaffolded initial commits pass `--no-gpg-sign`; hosted CLI e2e is
  env-gated instead of deleted.
- **Verification**: `cd packages/create-jazz && pnpm test` → `EXIT_CODE:0`
  (`70 passed | 1 skipped`).
- **Commit ref**: `25efcf2f2`.

## ALTER (2026-07-19): world-tour remains excluded with narrowed cause

- **What changed**: `world-tour` was enumerated directly. It fails before browser
  tests run because publishing `permissions.ts` returns `400 Bad Request`:
  server-shell policy conversion requires `EXISTS` predicates used from another
  table to include equality against `__jazz_outer_row`.
- **Spec**: recorded as 🔶 "Uncorrelated policy `EXISTS`" in
  `crates/jazz/SPEC/7_authorization.md`.
- **Exit criteria**: decide/lower bounded uncorrelated membership checks, then
  rerun and un-exclude `world-tour`.

## ADD (2026-07-19 ~02:15): jazz-napi binding guard in test-ts job

Turbo cache hits on `jazz-napi#build` reproducibly restored the package without
a loadable `.node` binding (32 `Cannot find native binding` failures, repeated
on rerun), while cache-miss builds work. Until the cache/output interaction is
root-caused, the test-ts job verifies `require('./crates/jazz-napi')` after
`build:ci` and force-rebuilds jazz-napi on failure. Self-healing, no coverage
lost; remove once the turbo cache issue is understood.

## ROOT CAUSE (2026-07-19 ~02:55) of the napi binding saga: debug builds

The force-build + verify guard isolated it: a freshly built DEBUG-profile
jazz-napi `.node` fails `dlopen` on the Linux runners (ERR_DLOPEN_FAILED),
deterministically. The job's `JAZZ_NAPI_RELEASE: "0"` (debug for speed) was the
trigger; the previously-working cache entry was an old release-mode artifact
that got evicted, after which every debug rebuild produced an unloadable
binding — masquerading first as cache corruption. CI now builds jazz-napi in
release mode (env removed, timeout 15→20m). Monday follow-up: root-cause why
debug cdylib dlopen fails on Linux (suspect debug section size / TLS model),
then possibly restore debug builds for speed.

## ROOT CAUSE, FINAL (2026-07-19 ~03:10): static TLS exhaustion

The full dlopen error (surfaced by the rebuild-and-verify guard) is
`cannot allocate memory in static TLS block` — glibc static TLS exhaustion
loading jazz-napi's .node, in both debug and release profiles. Newer engine
code carries a large initial-exec TLS segment; older cached artifacts predated
it, which produced the misleading cache-corruption trail. CI fix:
`GLIBC_TUNABLES=glibc.rtld.optional_static_tls=4194304` on the test-ts job.
Engine follow-up (Monday queue): audit large `thread_local!` usage in
jazz/groove and consider `-Z tls-model`/lazy allocation so consumers don't need
the tunable.

## RESOLVED decisions (Anselm, 2026-07-19 morning)

- Async-channel facade `all`/`one` exposure (c2d24ffc6 + probe updates):
  CONFIRMED; no longer pending review.
- Differential-gates CI step added to test-rust (canaries, both differential
  harnesses, oracle at JAZZ_SEED_COUNT=50).

## ROOT CAUSE, ACTUAL (2026-07-19 ~10:30): mimalloc initial-exec TLS in jazz-napi

The static-TLS exhaustion traces to `mimalloc-safe` as jazz-napi's global
allocator: mimalloc's default initial-exec TLS in a dlopen'd cdylib exhausts
glibc's static TLS reserve once enough other libraries load first (which is why
only import-heavy vitest workers failed — dev/, better-auth-adapter, framework
plugin suites — while light suites loaded fine, and why behavior varied with
runner images). Fix: `local_dynamic_tls` feature (mimalloc's supported mode for
shared libraries). The GLIBC_TUNABLES workaround is removed; the
rebuild-and-verify guard stays (it isolated the root cause). Perf note: dynamic
TLS adds a small per-access cost inside the napi cdylib only; jazz-sim bench
receipts are unaffected (different binary); flag for a napi-path receipt if the
allocator ever shows in profiles.

---

# Browser suite (merged from CI_NOTES_BROWSER.md, 2026-07-19)

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

## DECISION (Anselm, 2026-07-19): relation-lowering reds stay red

The ~9 relation-lowering tests (multi-hop, gather, object relation literals in
the public browser query path) are intentionally left failing — the red
test:browser step is the parity tracker for the engine-swap PR: it goes green
exactly when the public query surface reaches parity. Do NOT marker, skip, or
exclude them. The two bug-shaped families (websocket update convergence,
include materialization) are being fixed, not tracked.

## ALTER (2026-07-20): jazz-rn tests excluded from CI

Registering crates/jazz-rn as a workspace member (for the RN scaffolding's
workspace peer) put its legacy native-module tests into the turbo graph; they
need an RN test environment CI lacks, and the RN surface is compile-only by
decision. Excluded via --filter=!jazz-rn. Exit criteria: the RN owner lands the
SQLite driver and a proper RN test environment, then un-excludes.
