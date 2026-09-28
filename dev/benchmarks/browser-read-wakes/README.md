# Browser pending-read wakeups

Implementation: [#3614](https://github.com/garden-co/jazz/pull/3614).
Finding and outstanding follow-up: [#3612](https://github.com/garden-co/jazz/issues/3612).
The implementation starts at main `5f42b14f4d458bf3083694c17b629dcfd112d95b`;
it has no ancestry or runtime dependency on #3611. The standalone synthetic
profiling utility was reused from the diagnostic in #3613.

## What changed

Groove yields after a bounded work slice and wakes its caller. Previously,
`PendingNativeRead` discarded that wake and the TypeScript adapter resumed by
polling on a timer. Repeated browser timer clamping made already-ready work
wait between slices, even with memory storage.

The new boundary preserves the Rust wake and uses a MessageChannel host task
for each notified continuation. A suspended request does not repeatedly poll.
Core registers coverage and owner-lock waiters, and WASM supplies one cancellable
deadline timer. The read still checks the existing authority, scope, generation
and settlement conditions; a notification alone does not establish coverage.

Both browser databases, IVM, history, deletion and permission behavior remain.
Older bindings without `setWake` use the existing compatibility polling path.

## Verified isolated release-WASM comparison

Main: `5f42b14f4d458bf3083694c17b629dcfd112d95b`.
Candidate: `047ba66d7aafe79fed346790c93c984f55c5cecb`.
Exact source/binary hashes, manifests, raw samples, per-process identities,
setup phases and profile hashes are in [receipt.json](./receipt.json), under
`verifiedIsolatedComparison`. Both WASM hashes match the earlier built artifacts.

Synthetic public-API workload: 1,500 tasks sharing 15 folders; 128 repeated body
bytes per task and 2,048 repeated description bytes per folder, plus short labels.
One locally durable transaction, no remote Core server. Persistent mode retains
both the foreground database and durable SharedWorker/IndexedDB database. Checks
cover row identity/count and complete returned fields, including folder content.
This is a repeated read workload, not login latency or cold-start timing.

Each arm uses its exact source and its own release artifact. Artifacts were
preserved only inside their producing checkout; their original manifests were
verified at the exact producing revision on every switch. No build ran during
measurements. Three warmups and ten measured reads per lane, profiling disabled:

- Memory: main / candidate / candidate / main, four fresh processes total.
- Persistent: main / candidate / candidate / main / candidate / main / main /
  candidate, eight fresh processes total.
- Every run records a unique browser identity from the launched process's own
  endpoint, zero initial workers, owned Vite startup and verified browser shutdown.
- Separate CPU runs have one page profile in memory and one page plus one
  SharedWorker profile in persistent mode; no workers from earlier runs appear.

Median of process medians:

| Lane               |      Main | Candidate |                Result |
| ------------------ | --------: | --------: | --------------------: |
| Memory include     | 398.17 ms |  90.95 ms |      **4.38× faster** |
| Persistent include | 599.93 ms | 274.80 ms |      **2.18× faster** |
| Memory flat        |  33.57 ms |  35.11 ms | 4.6% slower, +1.54 ms |
| Persistent flat    | 169.70 ms | 165.25 ms | 2.6% faster, −4.45 ms |

The include improvement is repeatable and large. Flat reads do not show the
same class of improvement; the small differences and raw samples are retained.
The initial 10.4% persistent flat slowdown did **not** reproduce in this matched,
interleaved repeat. This is not evidence of a universal flat-read speedup.

## Separate CPU attribution and tradeoffs

One ten-read CPU capture per source/storage arm, 250 μs sampling. Values below
are sampled active milliseconds per read, excluding `(idle)` leaf samples.
Profiler start/stop edges remain in the windows, so these are approximate
attribution measurements, separate from the unprofiled latency comparison.

| Lane / thread               | Main active | Candidate active |
| --------------------------- | ----------: | ---------------: |
| Memory include / page       |    97.60 ms |        103.99 ms |
| Persistent include / page   |   126.47 ms |        130.39 ms |
| Persistent include / worker |   198.35 ms |        202.81 ms |
| Memory flat / page          |    37.36 ms |         45.04 ms |
| Persistent flat / page      |    68.30 ms |         68.78 ms |
| Persistent flat / worker    |   131.19 ms |        132.56 ms |

The latest include captures show **2–7% more sampled active CPU**, while wall
latency improves by 2.18–4.38×. This removes waiting; it does not reduce the
query engine's CPU work. Page and worker overlap and must not be summed as
elapsed latency. The memory flat profile is another negative control: its CPU
sample increases about 21%, while its unprofiled latency increases 1.54 ms.
There is only one CPU process per source/storage arm; these samples do not
establish a precise CPU regression magnitude across workloads.

Costs introduced by the implementation include a retained native waker, coverage
waiter bookkeeping, a cancellable deadline timer for suspended reads, and a lazy
MessageChannel for notified continuations. Sleeping reads do not repeatedly
poll. The earlier unconditional MessageChannel experiment's roughly doubled
foreground CPU is not observed in these captures. No functionality was removed.

## Earlier measurements and a discarded repeat

The first prototype comparison gave 4.12× memory includes and 2.10× persistent
includes, with a 10.4% persistent flat slowdown. Its earlier CPU captures showed
no include CPU increase. Those raw observations remain in
`earlierPrototypeComparison`; the verified isolated comparison above is the
current result, including its less favorable CPU measurements.

A follow-up fixed-port repeat was **discarded**. A browser left by a failed
launch still listened on port 9439; HTTP readiness accepted it before the new
browser bound its port. The later CPU captures included 18 and 20 SharedWorker
targets, exposing the contamination. Its faster flat numbers are not used.
The runner now obtains the debugging endpoint from its own child's stderr,
uses a dynamically assigned debugging port, verifies an initially empty worker
set, closes the browser through CDP and verifies process exit. It also waits
for its own Vite process to announce readiness and verifies its shutdown.

## Reproduce

This delay is specific to the browser scheduler; a native timing harness cannot
measure Chrome's timer clamping. Use a release workspace built in the checkout
being measured, retain runtime fingerprint checks, and avoid concurrent builds.
Do not copy generated bindings or manifests between checkouts.

```sh
pnpm build:core
node dev/benchmarks/browser-read-wakes/profile.mjs \
  --storage persistent --rows 1500 --iterations 10 \
  --out-dir target/browser-read-wakes/production-1
node dev/benchmarks/browser-read-wakes/profile.mjs \
  --storage persistent --rows 1500 --iterations 10 --cpu \
  --out-dir target/browser-read-wakes/production-cpu
node dev/benchmarks/browser-read-wakes/profile.mjs \
  --storage persistent --rows 1500 --iterations 10 --scheduling timer-polling \
  --out-dir target/browser-read-wakes/timer-control
```

Use `--storage memory` for the single-runtime control. `--root PATH` selects
another checkout and its own artifacts. Use unique output directories and
interleave source arms as above. `--trace` captures Chrome timer/task traces;
keep it and `--cpu` off for latency comparisons. Setup phases, warmups and
all measured samples are recorded separately.

The default executable discovery prefers Playwright's macOS headless shell;
`JAZZ_CHROMIUM_EXECUTABLE` selects an explicit binary on other platforms.
The debugging port defaults to zero (allocate a free port); the Vite port is
configurable. A failed launch or conflicting Vite listener fails the run instead
of attaching to an existing server. `browserIsolation.shutdownVerified` must be
true, and every process must have a distinct browser id before combining runs.

Diagnostic scheduling overrides are limited to this serial fixture. They are
not application code or replacements for concurrency/cancellation tests.
`message-channel` and `host-yield` preserve earlier rejected experiments; the
latter can time out on persistent coverage.

## Verification and limits

- Three Rust executor regressions pass: exact coverage wakes for concurrent
  readers, cancellation/deadline cleanup, and wake on owner-lock release.
- Seven new adapter tests pass, including zero repeated idle polls, wake
  coalescing, host timer fairness, independent reads, facade/owner shutdown,
  transport failures and pump failures. 148 existing runtime tests pass, with
  one existing skip.
- Six release-WASM contract tests pass, including the real coverage deadline
  and cancellation cleanup; two browser tests pass with concurrent includes,
  a queued write, MessagePort traffic, timers and animation frames in memory
  and persistent modes.
- All three incremental-delivery mechanism canaries pass. The bounded
  maintained-versus-one-shot oracle passes at 10 seeds, churn depths 10/1,000.
- Default-stack core tests abort on main as well as the candidate, including
  the catalogue offline lineage and deep causal replay tests. See #3331.
  A larger-stack full-library diagnostic reports 2,251 passes, one replay
  child timeout and four ignored tests. That unchanged replay test passes in
  9.91 seconds in isolation with the same larger stack. Neither diagnostic is
  a passing default-stack gate.
- Full canonical/CI-equivalent gates have not passed. The private sensitive-data
  guard was unavailable, not passed. This remains a draft; #3612 records the
  remaining validation and fixture compatibility work.

Tooling-friction: verified baseline snapshots within the producing checkout and
owned browser endpoints would have avoided the long rebuild and discarded repeat.
