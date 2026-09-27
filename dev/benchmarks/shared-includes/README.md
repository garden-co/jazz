# Shared-parent include profiling

Profiling PR: [#3613](https://github.com/garden-co/jazz/pull/3613). Implementation follow-up: [#3612](https://github.com/garden-co/jazz/issues/3612). Profiling follows #3609 / #3611. Current production source `a086c1be807f85022032e7a210ca796087b34603`; release WASM SHA256 `94d24bd3ecb24e15ce1a6b9ba7d8c8fd55f00aad70a7389aacce745255c04baf`. This is a separate profiling finding, with no runtime change shipped.

### Evidence

Public browser reads of 1,500 synthetic tasks sharing 15 folders, one locally durable pending transaction, no Core server. Includes retain the full folder description (2 KiB); tasks contain 128 bytes of body text. Persistent mode keeps both foreground and durable shared-worker databases.

- Memory include: roughly 74% of the sampled wall window is **idle**, despite all storage being resident.
- `EvaluationWorkQueue::poll` intentionally yields after a bounded CPU slice and calls `cx.waker().wake_by_ref()`.
- `WasmPendingNativeRead::poll_once` supplies `Waker::noop()`.
- `NativeRuntimeAdapter.awaitNativeRead` resumes by `await sleep(0)` after each pending poll.
- Chrome records **483 foreground timers clamped to 4 ms over five persistent include reads**, median actual delay 4.186 ms. Those waits can overlap other work; summing them would overstate additive wall cost.
- The sampled memory read uses the one-shot `query_relation_snapshot_in_authorization_mode` path. The earlier suspected per-result maintained transaction-vector clone is not the explanation for this lane.

### Causal experiment, profiling disabled

Replace only the pending-read loop's sleep with a MessageChannel task in the isolated harness. Same source/WASM/schema/rows/coverage; no production patch. Four fresh browser processes per storage mode in control/experiment/experiment/control order; ten measured reads per lane after three warmups. Median of process medians:

| Lane               |   Control | Experiment |       Ratio |
| ------------------ | --------: | ---------: | ----------: |
| Memory include     | 366.10 ms |   85.78 ms |       4.27× |
| Persistent include | 528.48 ms |  244.25 ms |       2.16× |
| Memory flat        |  32.50 ms |   32.46 ms |   unchanged |
| Persistent flat    | 129.02 ms |  135.95 ms | 5.4% slower |

### Do not ship the timer replacement as-is

It still polls while storage or coverage is unavailable. Matched ten-read CPU profiles show foreground active samples increasing from **122 to 256 ms/read** in persistent mode, despite lower wall time. The worker still needs about 143–147 ms of active work. A separate `scheduler.yield()` variant improves memory includes to ~89 ms but **times out waiting for persistent query coverage**. Its exact starvation mechanism needs an event-order test; a timeout is not evidence that coverage can be bypassed.

### Implementation direction

[#3612](https://github.com/garden-co/jazz/issues/3612) tracks a wake-driven read
boundary with fair host scheduling, coverage and deadline wakes, and cancellation/
transport-error handling. It includes the concrete implementation requirements
and verification cases. A native callback alone cannot replace polling while
coverage checks still return Pending without registering their caller's waker.

The remaining worker CPU is largely query/IVM execution and publication; this profile does not establish that IndexedDB disk I/O dominates. No history, deletion, permission, dual-database, or query-result semantics were removed.

## Reproduce

The runner owns a fresh Chromium process and profile, starts the existing
browser Vite configuration, and uses the verified workspace release WASM.
The original prototype wrapper produced `scheduling-receipt.json`; the checked-in
wrapper adds CLI arguments, setup-phase reporting, and a per-read count check.
Production source and WASM are unchanged. Its fixture uses the public schema,
transaction, local settlement and query APIs. Only the explicit diagnostic
scheduling modes override a private adapter method, within the owned test page.
They are intentionally restricted to serial reads and must not be copied into
application code.

```sh
pnpm --filter jazz-wasm build
node dev/artifacts/stage-native-fingerprints.mjs --workspace
node dev/benchmarks/shared-includes/profile.mjs \
  --storage persistent --rows 1500 --iterations 10 \
  --cpu --trace --out-dir target/shared-includes/control-profile
node dev/benchmarks/shared-includes/profile.mjs \
  --storage persistent --rows 1500 --iterations 10 \
  --scheduling message-channel --out-dir target/shared-includes/experiment
```

Use `--storage memory` for the single-runtime lane. Omit `--cpu --trace` for
wall-time comparison. Run four fresh processes in control/experiment/experiment/
control order, with unique output directories and no concurrent builds. Each
process warms the include and flat lanes separately, then records every sample.
Use `--scheduling host-yield` to reproduce the rejected Scheduler API experiment.
The recorded persistent coverage timeout is a failed experiment, not a pass.

The default Chromium discovery currently targets the macOS Playwright cache;
`JAZZ_CHROMIUM_EXECUTABLE` selects an explicit installed Chromium elsewhere.
The runner's Vite/CDP ports default to 4279/9439 and can be overridden. An initial
Vite dependency-discovery reload can interrupt page setup; its `vite.log` records
that case. A rerun after optimization completes uses the same cached dependencies.

Outputs include the exact source revision, WASM manifest/hash, harness and adapter
hashes, setup phases, warmup and measured samples, optional foreground/shared-worker
CPU profiles, and Chrome timer/task traces. CPU windows include profiler start/stop
edges and are not an additive per-read wall-time breakdown. Receipt profile entries
retain raw artifact SHA256 hashes; large raw trace files stay in the requested output
directory. No account secrets or seeded application rows are written to receipts.

### Reading the profile

For the 1,500-row persistent include, representative inclusive CPU samples per
read attribute about 55 ms to the foreground one-shot relation query, 12 ms to
TypeScript relation-result construction, and 17 ms to foreground authority update
ingestion. The shared worker spends about 89 ms in query update/publication,
including about 64 ms of IVM progress. These stacks overlap and **must not be
added**. Worker active samples are approximately 143 ms/read; foreground active
samples are approximately 122 ms/read in a separate untraced control profile.
The dominant avoidable wall delay in this fixture is the foreground timer loop.

There is no full canonical gate claim for this diagnostic-only change. Syntax,
focused lint, and live browser execution validate the harness. The existing
browser benchmark and its data assertions were left intact. The private
sensitive-data guard is unavailable locally; all fixtures here are synthetic.

Tooling-friction: an existing release WASM with symbols plus foreground/worker
CDP capture exposed this without another native build; a CPU-only flame graph
without idle and timer events would have missed the largest delay.
