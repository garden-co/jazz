# Jazz compile-boundary plan

## Objective

Reduce representative Jazz development, binding, and test build times by at
least 50% by creating one featureless semantic core and thin runtime, target,
and test shells. Do not split crates merely because source files are large.

This plan starts only after the semantic file-structure stack through the Node
runtime refactor has landed. Every proposed crate boundary must earn its cost
with measurements.

## Why this shape

Rust already incrementally caches work within crates at codegen-unit
granularity. Additional crate boundaries help when they isolate independently
changing or selected build units; they do not automatically accelerate a
serial dependency chain. Native and WASM builds target different triples and
cannot literally reuse compiled artifacts.

The opportunity in this workspace is narrower and more valuable:

- NAPI, native server, CLI, and WASM select different Jazz feature combinations;
- NAPI enables test and telemetry support in a production dependency;
- the native client feature depends on the server feature;
- storage, compression, telemetry, CLI dependencies, and large test harnesses
  sit too close to the semantic core;
- the large Jazz unit-test target makes focused white-box compilation expensive;
- concurrent worktrees create substantial incremental state and Cargo locking.

## Proposed package boundaries

```text
jazz                         platform-neutral public API and semantic runtime
├── groove                   generic records/schema/IVM/database over storage traits
├── optional jazz-protocol   only if measurements justify this low-level crate
└── no Tokio, HTTP, RocksDB, CLI, OTLP exporter, or test-harness dependencies

jazz-storage-rocksdb         native Groove/Jazz storage adapter
jazz-native-transport        Tokio, reqwest, and tungstenite client transport
jazz-server                  Axum routes, websocket server, JWT shell, serving config
jazz-cli                     clap, mimalloc, binaries; depends on jazz-server
jazz-testkit                 TestingClient, issuers, fixtures, oracle, simulation wiring
jazz-otel                    OpenTelemetry SDK/exporters and runtime setup
jazz-napi                    NAPI shell selecting native adapters
jazz-wasm                    WASM shell selecting browser adapters
```

Keep the package name `jazz` for the stable common API. Shell crates depend
inward; the core never depends outward.

### Semantic core

The common `jazz` crate retains IDs, time, transaction vocabulary, schema,
query AST and validation, semantic protocol frames, Node/peer/Db behavior,
result trees, wire framing, and generic storage/runtime contracts.

Keep tracing instrumentation calls in core, but not exporter SDKs or runtime
setup. Keep small white-box tests beside private implementation where private
access is valuable.

### Server, client, and CLI

- Move `serving` and server-specific `tools` modules to `jazz-server`.
- Move native client networking to `jazz-native-transport`.
- Remove the current `client -> server` feature dependency.
- Move binaries, clap configuration, signals, allocator selection, and command
  presentation to `jazz-cli`.

### Storage and compression

- Make generic storage the only core contract.
- Move RocksDB selection out of Jazz and Groove defaults into
  `jazz-storage-rocksdb`.
- Keep OPFS/browser storage in the WASM shell.
- Define uncompressed semantic framing plus a codec seam in core.
- Let shells select zstd, lz4, or ruzstd without recompiling semantic core.

### Telemetry

Core depends only on lightweight instrumentation vocabulary such as `tracing`.
`jazz-otel` owns OpenTelemetry SDKs, exporters, subscribers, and Tokio runtime
integration. Executable and binding shells opt into it.

### Tests

- Move `TestingClient`, `TestJwtIssuer`, reusable fixtures, simulation helpers,
  and the semantic oracle to `jazz-testkit`.
- Stop enabling `test-utils` and `otel-core` as NAPI production features.
- Keep focused private white-box tests in `jazz`.
- Move large public/scenario harnesses to `jazz-testkit` or dedicated integration
  packages so a focused core unit test does not compile every scenario fixture.
- Prefer package selection over production feature mutation for test behavior.

## Proposed feature model

The common core should approach:

```toml
[features]
default = []
sync-autopsy = []
cold-settle-attribution = []
```

The remaining diagnostic features should move outward if practical. Target
shells own adapter choices:

```toml
# jazz-server
[features]
default = ["zstd"]
zstd = ["jazz-compression/zstd"]
lz4 = ["jazz-compression/lz4"]

# jazz-wasm
[features]
default = ["ruzstd"]
bench-probes = []

# jazz-napi
[features]
default = ["rocksdb"]
rocksdb = ["jazz-storage-rocksdb"]
otel = ["jazz-otel"]
```

Remove the umbrella `test`, `test-utils`, `client`, `server`, `cli`,
`otel-core`, and `testing` features from semantic core after callers migrate.

## Canonical build matrix

| Build unit            | Target | Configuration                      |
| --------------------- | ------ | ---------------------------------- |
| semantic core         | native | featureless                        |
| semantic core         | wasm32 | featureless                        |
| core white-box tests  | native | `cfg(test)` only                   |
| native server and CLI | native | shell-selected storage/compression |
| NAPI                  | native | shell-selected storage/telemetry   |
| WASM                  | wasm32 | shell-selected OPFS/compression    |
| test harness          | native | separate dev package               |

Representative commands should become explicit and stable instead of relying
on large additive feature bundles.

## Measurement and acceptance gate

Before changing package boundaries, record clean and one-line incremental Cargo
timings for:

1. featureless Jazz core check;
2. Jazz core unit-test compilation;
3. Groove check and tests;
4. NAPI development and release artifact builds;
5. WASM development and release artifact builds;
6. native server and CLI checks;
7. one multi-lane shared-cache scenario.

Record rustc frontend, LLVM/codegen, native dependency, linker,
test-enumeration, and artifact-packaging time separately where possible.

Accept a vertical slice only when it:

- improves at least two representative workflows by 50% or more;
- does not materially slow the full canonical CI matrix;
- reduces, rather than multiplies, distinct Jazz feature artifacts;
- preserves public API and wire/storage compatibility;
- leaves a useful boundary even if later slices are rejected.

## Execution lanes

### Lane 1: measurement and cache inventory

Add reproducible timing commands and report current feature/artifact variants.
Measure shared-target contention and evaluate `sccache` separately from package
restructuring.

### Lane 2: testkit vertical slice

Extract reusable public scenario/test fixtures without widening core internals.
Measure focused core-test compilation and NAPI checks before and after.

### Lane 3: server, native transport, and CLI shells

Separate Axum/server, native client networking, and CLI dependencies. Remove the
`client -> server` edge. Measure minimal core, NAPI, and WASM builds.

### Lane 4: storage, compression, and telemetry adapters

Move RocksDB, compression implementations, and OTLP exporters to shell-selected
adapters. Verify native consumers reuse one identical core artifact.

### Lane 5: optional protocol crate experiment

Attempt only after earlier measurements. Low-level protocol/schema/query changes
fan out to every consumer, so this boundary has the weakest prior and must
demonstrate an additional 50% win.

## Stop conditions

Stop when a slice misses the 50% threshold, creates more feature variants than
it removes, requires broad visibility expansion, or makes compatibility
ownership less clear. Retain independent improvements such as testkit
separation, cache normalization, or server/CLI boundaries when useful alone.
