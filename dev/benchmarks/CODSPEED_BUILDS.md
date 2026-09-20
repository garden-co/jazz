# CodSpeed native benchmark build handoff

The three native workloads (todo, permissioned resources, policy-scoped
documents) compile on Blacksmith ARM64 Ubuntu 22.04, then run on the existing
`codspeed-macro` machines. Other suites are unchanged during this trial.
Build latency, cache behavior and acceptance receipts are tracked in
[#3174](https://github.com/garden-co/jazz/issues/3174).

## Invariants

- Rust 1.93.1, cargo-codspeed 5.0.1, the default `bench` profile, full CodSpeed
  debug info, allocator features and benchmark commands remain unchanged.
  `--locked` forbids dependency resolution drift. No `target-cpu=native`,
  optimization downgrade, debug stripping or fixture change. Rust's
  `--remap-path-prefix=$PWD=.` makes source paths workspace-relative: otherwise
  the profile has valid symbols but CodSpeed classifies Blacksmith's absolute
  checkout paths as unknown instead of project code. This is the one intentional
  debug-path flag difference; it is not a code-generation optimization.
- Each workload builds separately to avoid feature unification. A 16-vCPU
  build host replaces four compile jobs on a measurement host. This does not
  change benchmark execution parallelism or the measurement machine.
- `timed-cargo/cargo` forwards every argument and adds only `--timings` to
  the build subprocess (the pinned CodSpeed CLI cannot forward that option).
  Compiler timing HTML is uploaded even if the build fails. `/usr/bin/time`
  adds total resource-use receipts; the environment step identifies native
  compiler versions, memory and CPU availability.
- Caches are acceleration, not measurement authority: always run Cargo before
  sealing outputs. Workload/architecture/OS-separated caches retain workspace
  crates; the source SHA enters the environment hash so later builds can save
  updated workspace outputs while retaining cross-revision restore prefixes.
  Do not combine `key` with `shared-key`: the pinned action ignores `key` then.
  Standard GitHub branch isolation still applies. This does not pool different
  workloads' caches or grant PR caches authority over main.
- The JSON `jazz-codspeed-benchmark-artifact-v1` manifest binds the exact checkout
  SHA, GitHub workflow run ID, compiler identity, workload/build contract and
  SHA-256 of both executables. Consumer paths are fixed, not manifest-controlled.
  The receiver rejects a different source/run/compiler/contract, missing or
  modified files, symlinks, incompatible platform or missing ELF dependencies.
  It restores executable permissions lost by artifact upload only after hashing.
- These are benchmark executables, **not** the native/WASM correctness artifact
  store. No correctness producer/consumer or Turbo caching boundaries change.

## Examples and failure behavior

A todo build seals `target/codspeed/walltime/jazz-example-todo-benchmark/walltime`
and the Cargo CodSpeed CLI. The consumer checks out the same workflow SHA,
downloads only the named artifact from this workflow, verifies it and runs the
original `cargo codspeed run` command. It never compiles a fallback executable.

Rerunning a producer replaces its bundle for the same source/run; a failed
consumer can reuse a successful producer's bundle in a later attempt of the
same run. A new source revision gets a different artifact name. A build failure
prevents measurement; consumers must not fall back to the previous commit's
binary. As initially configured, the consumer matrix waits for the whole build
matrix, so the slowest producer gates all three consumers. Account for that
barrier, artifact transfer, cache upload and Cargo metadata dependency fetching
when comparing end-to-end latency, not just the compiler step.

`node --test dev/benchmarks/codspeed-artifact.test.mjs` checks handoff rejection
and timing-shim argument preservation. Hosted acceptance additionally requires
unchanged benchmark IDs/coverage, usable source-linked profiles after executable
relocation, measurement neutrality, and cold/warm build receipts. Matching OS
and architecture alone does **not** establish compiler-output equivalence;
native compiler versions and benchmark distributions must also be inspected.

No Jazz storage or sync wire format changes. The versioned JSON manifest is
short-lived CI provenance, not a persisted database or network protocol.
