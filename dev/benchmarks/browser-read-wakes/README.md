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

## Matched release-WASM comparison

Main: `5f42b14f4d458bf3083694c17b629dcfd112d95b`.
Candidate: `047ba66d7aafe79fed346790c93c984f55c5cecb`.
Each release artifact was built in the profiled checkout. Exact source, adapter,
harness, fixture and WASM hashes, manifests, raw timing samples and CPU-profile
hashes are in [receipt.json](./receipt.json).

Synthetic public-API workload: 1,500 tasks sharing 15 folders; 128 repeated body
bytes per task and 2,048 repeated description bytes per folder, plus short labels.
One locally durable transaction, no remote Core server. Persistent mode retains
the foreground database and durable SharedWorker/IndexedDB database. Every lane
checks row identity/count and full returned fields, including folder content.
This is a repeated read workload, not login latency or a cold-start benchmark.

Three warmups and ten measured reads per lane; two fresh browser processes per
source/storage arm, CPU profiling disabled. The summary is the median of the
two process medians. Main was measured before the candidate build; these source
arms were not interleaved.

| Lane               |      Main | Candidate |        Ratio |
| ------------------ | --------: | --------: | -----------: |
| Memory include     | 372.68 ms |  90.40 ms | 4.12× faster |
| Persistent include | 605.80 ms | 288.98 ms | 2.10× faster |
| Memory flat        |  33.68 ms |  33.80 ms |    unchanged |
| Persistent flat    | 152.62 ms | 168.57 ms | 10.4% slower |

The flat-read result is a negative control, not a general speedup claim.

### Scheduler control on the same candidate WASM

Four fresh persistent browser processes, old timer loop / production wake loop /
production wake loop / old timer loop. Each uses the same candidate WASM, schema,
rows and ten-read measurement protocol. Only the diagnostic foreground adapter
loop changes; it never calls `setWake` in the timer arm.

| Lane               | Timer polling | Wake driven |             Ratio |
| ------------------ | ------------: | ----------: | ----------------: |
| Persistent include |     584.56 ms |   287.01 ms |      2.04× faster |
| Persistent flat    |     172.18 ms |   172.38 ms | unchanged (0.12%) |

This isolates the large include win to scheduling and does not reproduce the
flat slowdown from changing the scheduler alone. It does **not** distinguish
environment/time variation from other candidate-versus-main binary effects.
The initial 10.4% flat result remains unresolved in #3612.

## Separate CPU attribution

One ten-read CPU capture per source/storage arm, 250 μs sampling. Numbers below
are sampled active milliseconds per read, excluding `(idle)` leaf samples.
Profiler start/stop edges are included, so these are approximate attribution
windows, separate from the unprofiled latency comparison.

| Lane / thread               | Main active | Candidate active |
| --------------------------- | ----------: | ---------------: |
| Memory include / page       |   100.25 ms |         98.56 ms |
| Persistent include / page   |   129.37 ms |        125.39 ms |
| Persistent include / worker |   181.18 ms |        167.72 ms |
| Persistent flat / page      |    69.05 ms |         69.35 ms |
| Persistent flat / worker    |   116.53 ms |        144.24 ms |

For memory includes, page idle samples fall from 267.65 to 0.41 ms/read.
Persistent include page idle falls from 464.87 to 164.20 ms/read. Page and worker
work overlap: their samples must not be added as elapsed latency. The include
improvement is primarily less waiting; the remaining worker query/IVM cost is
still present. There is no measured include CPU increase in this capture.
The flat worker increase is also retained as a negative result.

The earlier unconditional MessageChannel polling experiment in #3613 doubled
foreground CPU while waiting externally. This implementation waits for actual
notifications and avoids that measured failure mode.

## Reproduce

Use a release workspace built in the checkout being measured. Keep runtime
fingerprint checks enabled and do not copy generated bindings between checkouts.
No builds should run alongside timing measurements.

```sh
pnpm build:core
node dev/benchmarks/browser-read-wakes/profile.mjs \
  --storage persistent --rows 1500 --iterations 10 \
  --out-dir target/browser-read-wakes/production-1
node dev/benchmarks/browser-read-wakes/profile.mjs \
  --storage persistent --rows 1500 --iterations 10 --scheduling timer-polling \
  --out-dir target/browser-read-wakes/timer-1
node dev/benchmarks/browser-read-wakes/profile.mjs \
  --storage persistent --rows 1500 --iterations 10 --cpu \
  --out-dir target/browser-read-wakes/production-cpu
```

Use `--storage memory` for the single-runtime control. `--root PATH` selects
another checkout and its own locally built artifacts, so the same runner can
profile main without adding benchmark source to that checkout. Use unique
output directories and interleave source arms where possible. `--trace` records
Chrome timer/task traces; keep it and `--cpu` off for latency comparisons.

The runner defaults to Chromium from the macOS Playwright cache;
`JAZZ_CHROMIUM_EXECUTABLE` selects a Chromium binary elsewhere. It owns a fresh
browser/profile and local Vite server, with configurable Vite/CDP ports. Receipts
include setup phases, every warmup/measured sample and optional raw CPU profiles.
The preserved runner adds CLI/setup reporting and a per-read count check to the
prototype used for the recorded comparison. Prototype hashes remain in the
receipt; they are not presented as hashes of this later wrapper.

Diagnostic overrides are confined to the owned serial fixture. They are not
application code and are not a replacement for concurrency/cancellation tests.
`message-channel` and `host-yield` preserve the earlier rejected controls; the
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
  remaining validation and flat-read comparison work.

Tooling-friction: retaining independently built main/candidate WASM artifacts
within their producing checkouts and fixing one compiler/SDK environment would
avoid the long rebuilds that dominated this investigation.
