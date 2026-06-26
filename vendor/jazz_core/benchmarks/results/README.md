# Retained Benchmark Results

This directory holds small benchmark captures that are useful to quote in
docs, PRs, and follow-up comparisons. Treat these files as retained evidence,
not as a complete benchmark database.

## Status Classes

- **Quoteable**: checked-in JSONL with metadata, a clean or intentionally noted
  dirty state, and a clear run id or filename. Prefer these for public claims.
- **Stale**: still valid as historical context, but not representative of the
  current code path. Keep only when it anchors a comparison or regression note.
- **Dirty**: captured with `git_dirty: true` or local knobs that materially
  change behavior. Do not quote without saying exactly what was dirty.
- **Merged**: assembled from fresh rows plus a frozen baseline so plots can
  compare current work against an older implementation. Quote the fresh rows
  normally; identify baseline rows as frozen.
- **Frozen**: old baseline rows kept for continuity. Do not refresh in place;
  create a new run file when recapturing.

JSONL rows should include at least `git_sha`, `git_dirty`, host/platform or
toolchain metadata when the runner supports it, and enough scenario knobs to
re-run the workload.

When a benchmark writes retained output into this directory while it is running,
set `JAZZ_BENCH_IGNORE_RESULT_DIRTY=1` so jazz-sim metadata ignores
`benchmarks/results/**` as a source of dirtiness. This does not hide source-code
changes; it only prevents the output file being captured from making its own
rows dirty.

## Current Index

- `headline-v2.jsonl`: quoteable Groove headline curve.
- `headline-v2-groove.jsonl`: merged/frozen comparison material for the Groove
  headline curve.
- `headline-v0.jsonl`, `headline-v1.jsonl`: stale headline captures retained
  for historical comparisons.
- `acl-v0.jsonl`, `acl-v1.jsonl`: ACL curve captures; check row metadata before
  quoting because older rows may be stale.
- `oneshot-v0.jsonl`: stale one-shot capture.
- `jazz/*.jsonl`: Jazz package/scenario captures. Many existing rows are dirty
  retained results; inspect `git_dirty`, `git_sha`, and `knobs` before quoting.

## Capturing Jazz Package Benches

Use the wrapper for package-level Jazz benches that emit either JSON or stable
text output:

```sh
JAZZ_BENCH_RUN_ID=jazz-sync-YYYYMMDD scripts/bench_jazz_package.py sync \
  > benchmarks/results/jazz/sync-YYYYMMDD.jsonl

JAZZ_DEPTHS=1000 JAZZ_PENDING_SIZES=0 \
  JAZZ_BENCH_RUN_ID=jazz-cold-subscription-YYYYMMDD \
  scripts/bench_jazz_package.py cold_subscription \
  > benchmarks/results/jazz/cold_subscription-YYYYMMDD.jsonl
```

Supported benches:

```sh
scripts/bench_jazz_package.py --list
scripts/bench_jazz_package.py --print-command
```

The wrapper currently covers `sync` and `cold_subscription`; it adds run
metadata to each JSONL row and normalizes `cold_subscription` durations to
microseconds. Do not run full captures casually: set the bench-specific
environment variables to a small smoke profile when checking the command path.

## Cost Tiers

- **Smoke**: tiny knob settings used to verify a benchmark still runs and emits
  parseable JSONL. Useful during implementation.
- **Fast**: `JAZZ_BENCH_PROFILE=fast` selects interactive defaults while still
  running the normal benchmark path and emitting retained rows. Explicit
  bench-specific `JAZZ_*` knobs override the fast defaults.
- **Profile**: `JAZZ_BENCH_PROFILE=profile` selects a middle tier intended for
  symbolized profiler captures. It keeps the same benchmark phases as full, but
  uses smaller-but-nontrivial cardinalities, repetitions, and fanout. Explicit
  bench-specific `JAZZ_*` knobs override the profile defaults.
- **Retained snapshot**: current-code rows with clean metadata and moderate
  repetition/scale. Useful for regression direction and planning.
- **Scale claim**: expensive, repeated runs at product-derived scale. Use only
  when making or validating a performance claim.

Simulator transport codec selection defaults to native in-memory delivery. Set
`JAZZ_TRANSPORT_CODEC=native|wire_bytes|wire_frames` to choose a global default
for benches that exercise transport codecs. Scenario-specific knobs such as
`JAZZ_S1_RECONNECT_TRANSPORT_CODEC` and `JAZZ_S2_TRANSPORT_CODEC` take
precedence over the global default.

## Symbolized Jazz Profiling

For CPU profiles, build bench executables with debug symbols and run the bench
binary directly under `samply`:

```sh
scripts/profile_jazz_bench.sh s2_canvas
```

The helper sets `CARGO_PROFILE_BENCH_DEBUG=true` for the build, defaults to
`JAZZ_BENCH_PROFILE=profile` for the capture, and writes
`target/profiles/<bench>-profile.json.gz` without opening the profiler UI.
Override any bench-specific knob as usual when you need a narrower or broader
target:

```sh
JAZZ_S2_SECONDS=3 JAZZ_S2_RATE=8 scripts/profile_jazz_bench.sh s2_canvas
```

To inspect or run the executable manually, build it first:

```sh
CARGO_PROFILE_BENCH_DEBUG=true \
  cargo bench -p jazz-sim --bench s2_canvas --no-run --message-format=json
```
