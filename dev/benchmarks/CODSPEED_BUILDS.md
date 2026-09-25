# CodSpeed native benchmark build handoff

Every CodSpeed walltime workload compiles on Blacksmith ARM64 Ubuntu 22.04,
then runs on the existing `codspeed-macro` machines. The ten workloads are the
five native examples (todo, permissioned resources, policy-scoped documents,
BandChat, WorldTour), BigLabel (ingest and loads), W1 (memory, RocksDB and
ahead-current), route subscription, the Groove IVM experiment and maintained
selective hydration. `workloadSpecs` in `codspeed-artifact.mjs` is the one
table of each workload's package, benches, build-time features, measurement
thread stack and timeout. The workflow's plan job reads its `matrix` and
`measure` output, and the build and measurement jobs read `build-args` and
`run-args`, rather than repeating them. Build latency, cache behavior and acceptance receipts are tracked in
[#3174](https://github.com/garden-co/jazz/issues/3174).

## Invariants

- Rust 1.93.1, cargo-codspeed 5.0.1, the default `bench` profile, full CodSpeed
  debug info, per-workload features (the mimalloc allocator for the native
  examples, `testing` for the two `jazz` benches) and benchmark commands remain
  unchanged. The measurement job keeps each workload's former environment:
  only the native examples set `RUST_MIN_STACK`, and each keeps the timeout its
  former build-and-run job had.
  `--locked` forbids dependency resolution drift. No `target-cpu=native`,
  optimization downgrade, debug stripping or fixture change. Rust's
  `--remap-path-prefix=$PWD=/actions-runner/_work/jazz/jazz` maps source paths
  to the measurement runner's observed absolute checkout root. Both the original
  Blacksmith paths and the attempted relative `./crates/...` paths produced
  valid symbols but `origin: unknown` in full GQL profiles. CodSpeed uploads its
  absolute repository root; the new trial matches it rather than relying on
  relative-path normalization. `codspeed-artifact.mjs rustflags` owns this mapping,
  the artifact contract pins it, and installation rejects checkout-path drift.
  This is the one intentional debug-path flag difference; it does not change
  optimization settings. Hosted user-source attribution passed at `9c7995a1d41189ae0719e2cdb442423c9b75f233`: CodSpeed run `6aaf3c6cc8296f49201b8f39` classified all 54,073 cold-sync project frames and 5,539 sequential-update project frames as user code, with zero non-repository paths. Result IDs are `6aaf3ee7ad9a6239bfb27f3b` and `6aaf3ee7ad9a6239bfb27f37`, respectively.
- Each workload builds separately to avoid feature unification. A 16-vCPU
  build host replaces four compile jobs on a measurement host. This does not
  change benchmark execution parallelism or the measurement machine.
- `timed-cargo/cargo` forwards every argument and adds only `--timings` to
  the build subprocess (the pinned CodSpeed CLI cannot forward that option).
  Compiler timing HTML is uploaded even if the build fails. `/usr/bin/time`
  adds total resource-use receipts; the environment step identifies native
  compiler versions, memory and CPU availability.
- Caches are acceleration, not measurement authority: always run Cargo before
  sealing outputs. Rust-cache retains registry, Git and installed-tool caches
  with target caching disabled. Explicit cache restore/save steps retain
  `target/release`, including workspace outputs. Their compatibility prefix
  pins workload (and so its features), OS/architecture, Ubuntu image, Rust/CodSpeed versions,
  absolute debug-path contract, Cargo lockfile, toolchain file and Cargo config.
  Only the primary save key appends the source SHA; the restore prefix does not,
  so a new revision can restore and then save refreshed outputs. The pinned
  rust-cache action cannot express this with `key`, `shared-key` or `env-vars`:
  all affect its restore prefix too. Standard GitHub branch isolation still
  applies. Receipt bundles are excluded; caches never authorize measurement.
- The JSON `jazz-codspeed-benchmark-artifact-v2` manifest binds the exact checkout
  SHA, GitHub workflow run ID, compiler identity, workload/build contract
  (including package, benches and features) and SHA-256 of every bench
  executable plus the CLI. v1 bundles, which held one `walltime` executable, are
  rejected. Consumer paths are fixed, not manifest-controlled.
  The receiver rejects a different source/run/compiler/contract, missing or
  modified files, symlinks, incompatible platform or missing ELF dependencies.
  It restores executable permissions lost by artifact upload only after hashing.
- Every bench executable must contain both `.debug_info` and `.debug_line`. The checkout
  supplies source files at the same SHA; Cargo intermediates are not needed for
  these embedded debug sections. This is not a split-debug-artifact protocol.
- These are benchmark executables, **not** the native/WASM correctness artifact
  store. No correctness producer/consumer or Turbo caching boundaries change.

## Examples and failure behavior

A todo build seals `target/codspeed/walltime/jazz-example-todo-benchmark/walltime`
and the Cargo CodSpeed CLI. The consumer checks out the same workflow SHA,
downloads only the named artifact from this workflow, verifies it and runs the
original `cargo codspeed run` command. It never compiles a fallback executable.
A W1 build seals both `reads_memory_walltime` and `reads_rocksdb_walltime`; a
bundle missing either, or sealed for another workload, is rejected.

Rerunning a producer replaces its bundle for the same source/run; a failed
consumer can reuse a successful producer's bundle in a later attempt of the
same run. A new source revision gets a different artifact name. A build failure
prevents measurement; consumers must not fall back to the previous commit's
binary. As initially configured, the consumer matrix waits for the whole build
matrix, so the slowest producer gates all ten consumers. Account for that
barrier, artifact transfer, cache upload and Cargo metadata dependency fetching
when comparing end-to-end latency, not just the compiler step. One workload's build failure
skips no other workload's measurement: the consumer matrix runs unless the
whole build was skipped or cancelled, and only the failed workload's download
fails.

`node --test dev/benchmarks/codspeed-artifact.test.mjs` checks handoff rejection
and timing-shim argument preservation. Hosted acceptance additionally requires
unchanged benchmark IDs/coverage, usable source-linked profiles after executable
relocation, measurement neutrality, and cold/warm build receipts. Matching OS
and architecture alone does **not** establish compiler-output equivalence;
native compiler versions and benchmark distributions must also be inspected.

No Jazz storage or sync wire format changes. The versioned JSON manifest is
short-lived CI provenance, not a persisted database or network protocol.
