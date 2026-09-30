# Tracing: always compiled in, tuned at runtime

Jazz instruments itself with the [`tracing`](https://docs.rs/tracing) crate
and nothing else. Instrumentation on synchronous code is never behind a Cargo
feature; async phases are the one open exception (below). Detail is
chosen at runtime by whoever listens, and a call site nobody listens to costs a
load and a branch.

## Why `tracing` meets the three goals

- **Always compiled in.** `trace_span!`, `#[instrument]` and `trace!` expand to
  a `static` callsite. No Cargo feature, no `cfg`.
- **Tunable detail.** Every call site has a target and a level. A subscriber's
  filter picks targets and levels at runtime, and a `reload` handle can change
  them while the process runs.
- **Zero cost when nobody listens.** A disabled call site does three things.
  It compares against the compile-time `STATIC_MAX_LEVEL`, which is free. It
  does one relaxed load of the global `LevelFilter`, which is `OFF` until a
  subscriber is installed. If the level passes, it does one relaxed load of the
  callsite's cached `Interest`, which is `never` for a filtered-out target. There
  is no clock read, no allocation, and no field evaluation. Field expressions
  are evaluated only after the call site is enabled.

The cost people see with `cold-settle-attribution` (1.7 s of 20 s on the
permissioned cold load, `dev/benchmarks/profile-observer-overhead.md`) is the
listener's cost: about 1.28M clock reads for 641k span entries. It is not the
cost of the call sites. Keeping call sites always compiled in and paying only
when a listener is attached is the model `tracing` is built for.

## Targets and levels

| Target                            | Level       | What                                                                                            |
| --------------------------------- | ----------- | ----------------------------------------------------------------------------------------------- |
| `jazz`, `jazz_tools`, `groove`, … | error…debug | Ordinary logs, chosen by the log level.                                                         |
| `jazz::profile`                   | debug       | Phases: one span per pipeline step (`query_prepare`, `ivm_tick`, `storage_apply`, `ingest`, …). |
| `jazz::profile`                   | trace       | Fine detail: per-poll and per-operator-family spans on the IVM hot path.                        |
| `jazz::debug::<topic>` (proposed) | trace       | Replaces the `JAZZ_*_TRACE` env switches.                                                       |

`jazz::profile` is opt-in by name. A blanket `RUST_LOG=trace` or wasm
`logLevel: "trace"` does not enable it, because timing every kernel call is a
profiling decision, not a verbosity decision:

- Native (server, CLI, napi with a collector): `RUST_LOG=jazz::profile=debug`
  for phases, or `=trace` for operators. The EnvFilters in `jazz-cli` and
  `jazz-otel` add `jazz::profile=off` unless `RUST_LOG` names the target.
- Wasm: the console layer excludes `jazz::profile`. A later `DbConfig.profile:
"phases" | "operators"` can install a phase-totals layer and read it back.
- Benchmarks: the `jazz-sim` phase `Collector` matches the `cold.phase.*` span
  names directly, whatever the target.

Span names stay `cold.phase.*` for now so the existing collector and reports
keep working. A rename to plain phase names can follow once the collector no
longer depends on them.

## Rules for call sites

1. Never read the clock at a call site. Put a span around the region, and let
   the subscriber time it.
2. Never compute an expensive value outside the macro. Pass it as a field
   (`plan = %plan`) so it is evaluated only when enabled. Guard multi-statement
   preparation with `if tracing::enabled!(target: "jazz::profile", Level::TRACE)`.
3. Per-row work accumulates into a local and emits one event per batch.
4. Counters are events with fields, aggregated by a layer, not process-global
   atomics bumped on every call. Anything beyond a sum belongs in the layer
   too: a distinct count is an `identity` field that the layer inserts into a
   set, and grouping by node role reads the enclosing role span.
5. Spans on synchronous code are always compiled in. Spans on `async fn`s
   are not yet; see the next section.
6. No `cfg(feature = …)` around instrumentation. A Cargo feature is for a
   listener that pulls in a heavy dependency (pprof, an allocator hook), never
   for the call sites.

## Async phases: the open problem

A span on synchronous code costs a branch when disabled. A span on an `async
fn` is different, because it must be entered on every poll, so something has
to wrap the future. Any wrapper makes the future bigger even when nobody
listens:

- `#[tracing::instrument]` on an `async fn` awaits the body on two paths
  (instrumented and not), so the generated future holds the body more than
  once.
- A single `Instrument::instrument(async move { … }, span)` still moves the
  body future through temporaries. In unoptimized builds each temporary is its
  own stack slot.

Both overflowed the stack on the cold-load call chain in debug builds. The
native debug `m3_maintained_one_shot_differential_oracle` test and the wasm
TypeScript suite (silent shadow-stack corruption, then `memory access out of
bounds` on the next call) both failed. So the async phase spans (`query_prepare`, `ivm_tick`,
`storage_apply`, `ingest`, …) stay behind `cold-settle-attribution` for now.

Two ways forward, to be measured before either lands:

- **Scope phases to synchronous regions.** Most phase time is CPU work between
  awaits. Entering a sync span around those regions keeps the zero-cost
  property and needs no wrapper. The cost is that awaited child work is
  attributed to the child's own spans, not the parent phase.
- **Box only when listening.** The disabled path awaits the body inline, and
  the enabled path boxes it before wrapping. The disabled future grows by a
  span and a pointer, and only a profiled run pays an allocation per phase
  entry. This needs a measured frame-size check in debug builds.

## Measured cost when nobody listens

Callgrind instruction counts, tracing 0.1.44, release build:

| Setup                                          | Instructions per disabled `trace_span!(…).entered()` |
| ---------------------------------------------- | ---------------------------------------------------- |
| No subscriber installed                        | 25                                                   |
| Subscriber at `info` (the jazz-cli default)    | 25                                                   |
| Subscriber at `trace` with `jazz::profile=off` | 32                                                   |

On the groove `steady_state` IVM bench (the cases CodSpeed measures, 1,800
`Engine::step` calls), the synchronous spans are entered about 9 times per
step against about 74M instructions per step: roughly 225 instructions, or
0.0003%. That is far below the bench's own run-to-run spread under callgrind
(about 0.5%, from hash seeds), and the base and prototype totals fall inside
that spread. On the permissioned cold load, 641k span entries would cost about
16M instructions against an 18.7 s run.

## Migrating what exists today

| Today                                                                                                                                                                                          | Gate                      | Becomes                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Synchronous `cold.phase.*` spans (groove, jazz-node, jazz-db)                                                                                                                                  | `cold-settle-attribution` | Always compiled, `jazz::profile` target. **Done in this change.** Spans on `async fn`s stay gated; see above.                                                                                                                                                                                                                                                                                                                |
| Operator phases added by #3786 and #3821 (`op_*`, `pending_*`, `op_recursive`)                                                                                                                 | same                      | Same as above for the synchronous ones (`op_*`, `pending_*`). `op_recursive` in #3821 is on an `async fn`, so it waits for the async answer above.                                                                                                                                                                                                                                                                           |
| groove `cold_settle_attribution` counters (pipeline/map/join cardinality, map buffer, per-node map work)                                                                                       | same                      | `trace!` events on `jazz::profile` with the same fields. A `CardinalityLayer` in jazz-sim rebuilds today's `Snapshot`. Per-node `elapsed_ns` becomes a span with `node`/`hydrate` fields that the layer times.                                                                                                                                                                                                               |
| Row-representation conversion counters (#3791: per site and node role, `calls` vs `distinct` physical versions, per-column-family table writes)                                                | same                      | `trace!` events on `jazz::profile` with `site`, `identity` and `bytes` fields. The identity hash sits inside the macro, so it is computed only when a listener is attached. The node role comes from the enclosing `cold.phase.core`/`relay`/`client` span, which removes the thread-local `set_role`. The table-write event belongs on the storage write path, not only the commit path, so it counts every physical write. |
| jazz-db `cold_settle_attribution` counters (preflight encodes, view-update splitting)                                                                                                          | same                      | Same as above. The encode timings become spans.                                                                                                                                                                                                                                                                                                                                                                              |
| `bench-alloc-sites`                                                                                                                                                                            | feature (backtrace)       | Stays a feature, since it installs an allocator hook. It already reads the current phase from the phase layer, which is the right shape.                                                                                                                                                                                                                                                                                     |
| jazz-sim `profiling` (pprof flamegraphs)                                                                                                                                                       | feature (pprof)           | Stays a feature for the pprof dependency. Phase boundaries come from spans instead of `maybe_profile_phase`.                                                                                                                                                                                                                                                                                                                 |
| `r3-open-attribution`                                                                                                                                                                          | feature                   | Spans on each `Db::open` step. The bench reads them through the phase layer, which removes `open_with_receipt_for_test`.                                                                                                                                                                                                                                                                                                     |
| `sync-autopsy` ring buffer                                                                                                                                                                     | feature                   | `trace!` events on `jazz::debug::sync_autopsy`. The test helper installs a ring-buffer layer scoped to the test.                                                                                                                                                                                                                                                                                                             |
| `JAZZ_COVERED_INPUT_TRACE`, `JAZZ_REHYDRATE_TRACE`, `JAZZ_QUERY_TEMPLATE_TRACE` (debug*env) and the raw `GROOVE_TRACE*\*`, `JAZZ_CLOSURE_TRACE`, `JAZZ_CAPABILITY_TRACE`, `JAZZ_HISTORY_DEBUG` | env var + `eprintln!`     | `trace!` on `jazz::debug::<topic>`, enabled by `RUST_LOG`. `benchmark-guard` then refuses runs whose filter enables `jazz::profile` or `jazz::debug`, instead of keeping a list of env var names.                                                                                                                                                                                                                            |
| Peer IO auxiliary trace (`setAuxiliaryTraceEnabled`)                                                                                                                                           | runtime flag              | Events on `jazz::debug::peer_io`, drained through the existing wasm trace-entry queue.                                                                                                                                                                                                                                                                                                                                       |
| Always-on metrics structs (`SyncMetrics`, `TickMetrics`, storage metrics, …)                                                                                                                   | none                      | Unchanged. They are cheap product counters. Export them as OTel meters later.                                                                                                                                                                                                                                                                                                                                                |
| TS `__JAZZ_SUBSCRIPTION_TRACE__`                                                                                                                                                               | global                    | Out of scope here. A small TS helper can mirror the same target/level config later.                                                                                                                                                                                                                                                                                                                                          |

The mimalloc sampling wrapper proposed in the server profiling work fits the
same model. Each sampled allocation reads the current phase from the phase
layer, as `bench-alloc-sites` already does, so heap samples are attributed to
phases in production without a special build.
