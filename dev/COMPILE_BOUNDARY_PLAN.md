# Jazz compile-boundary plan

## Objective

Create one featureless semantic core with thin runtime, target, and test shells.
The resulting package graph should make ownership and supported build
configurations obvious while reducing redundant compilation as a consequence.
Do not split crates merely because source files are large.

This plan starts only after the semantic file-structure stack through the Node
runtime refactor has landed. A proposed crate boundary earns its cost by making
dependency direction, target ownership, or feature selection materially
clearer; measurements are useful feedback, not a prerequisite.

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

Progress: JWT verification is now selected by the server capability rather
than compiled as an unconditional semantic-core dependency. Identity signing
remains in core because it is part of client identity semantics, not the HTTP
authentication adapter.

### Storage and compression

- Make generic storage the only core contract.
- Move RocksDB selection out of Jazz and Groove defaults into
  `jazz-storage-rocksdb`.
- Keep OPFS/browser storage in the WASM shell.
- Define uncompressed semantic framing plus a codec seam in core.
- Let shells select zstd, lz4, or ruzstd without recompiling semantic core.

Progress: codec implementations and their native/pure-Rust dependencies now
live in featureless-by-default `jazz-compression`. Jazz retains wire feature
negotiation, envelope semantics, and logical message limits; its compatibility
features forward codec selection to the adapter crate. This is the first half
of the boundary: shells still select the established Jazz features until codec
choice can be injected without weakening wire negotiation.

### Telemetry

Core depends only on lightweight instrumentation vocabulary such as `tracing`.
`jazz-otel` owns OpenTelemetry SDKs, exporters, subscribers, and Tokio runtime
integration. Executable and binding shells opt into it.

Progress: the NAPI shell now delegates process-global subscriber assembly and
tracer-provider lifetime ownership to `jazz-otel`; it no longer depends
directly on `opentelemetry_sdk` or `tracing-subscriber`. Shell-specific default
filter directives remain explicit inputs to the adapter boundary.

The NAPI shell also selects the narrowly named `embedded-server` capability
instead of Jazz's integration-test umbrella. This keeps the embedded native
server/client/SQLite surface it actually exposes without compiling sync-autopsy
instrumentation into the production binding artifact.

`jazz-testkit` now distinguishes its featureless duplex-transport base from
public client/server scenarios and their native adapters. Direct testkit runs
default to the full scenario harness, RocksDB, and zstd as before, while Jazz's
white-box dev-dependency disables those defaults. Its retained engine tests can
use the duplex transport without re-enabling server, client, SQLite, RocksDB,
compression, HTTP/JWT, async-runtime, or fixture dependencies through feature
unification. Those dependencies are optional and selected by `scenarios`.

### Tests

- Move `TestingClient`, `TestJwtIssuer`, reusable fixtures, simulation helpers,
  and the semantic oracle to `jazz-testkit`.
- Make NAPI depend directly on `jazz-otel`; stop enabling telemetry support in
  the semantic crate. Its remaining `test-utils` dependency belongs to the
  later testkit slice.
- Keep focused private white-box tests in `jazz`.
- Move large public/scenario harnesses to `jazz-testkit` or dedicated integration
  packages so a focused core unit test does not compile every scenario fixture.
- Prefer package selection over production feature mutation for test behavior.

Progress: reusable scenario-client, JWT, permissions-publication, query-wait,
and in-memory duplex helpers now live in `jazz-testkit`. Existing Jazz
integration targets consume that package through a dev-dependency, so helper
changes compile as one independently cached unit while the scenario targets
are migrated outward incrementally. The first outward cluster covers public
claims, inherited policies, policy-aware branches, authorization across schema
renames, scope revocation, and local-first authentication; these seven binaries
now compile and run under `jazz-testkit` rather than Jazz's core test inventory.
The remaining public client/server, HTTP admission, synchronization, schema
evolution, persistence, subscription, and transaction scenarios followed as
one migration. Direct `Db`, Node, peer, wire, IVM, and storage contract targets
remain under Jazz even when Cargo represents them as integration binaries.

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

Remove the remaining umbrella `test`, `test-utils`, `client`, `server`, and
`testing` features from semantic core after callers migrate. The executable
and telemetry slices remove `cli` and `otel-core` first.

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

## Verification and performance feedback

As the package boundaries settle, record representative clean and incremental
Cargo timings for:

1. featureless Jazz core check;
2. Jazz core unit-test compilation;
3. Groove check and tests;
4. NAPI development and release artifact builds;
5. WASM development and release artifact builds;
6. native server and CLI checks;
7. one multi-lane shared-cache scenario.

Record rustc frontend, LLVM/codegen, native dependency, linker,
test-enumeration, and artifact-packaging time separately where possible.

Accept a vertical slice when it:

- makes semantic, target, adapter, or test ownership materially clearer;
- does not materially regress the full canonical CI matrix;
- reduces, or provides a direct path to reducing, distinct Jazz feature artifacts;
- preserves public API and wire/storage compatibility;
- leaves a useful boundary independent of later performance work.

Timing improvements remain an expected outcome and should guide later tuning,
but an otherwise sound architectural boundary does not need to prove a fixed
percentage improvement before landing.

## Execution lanes

### Lane 1: executable shells

Extract process entry points, command parsing, signals, allocator selection,
and presentation dependencies from the semantic crate without changing binary
names or behavior.

### Lane 2: testkit vertical slice

Extract reusable public scenario/test fixtures without widening core internals.
Measure focused core-test compilation and NAPI checks before and after.

### Lane 3: server, native transport, and CLI shells

Separate Axum/server, native client networking, and CLI dependencies. Remove the
`client -> server` edge. Measure minimal core, NAPI, and WASM builds.

Progress: executable binaries now live in `jazz-cli`, native client networking
no longer selects the server module, and `jazz` is featureless by default.
Actual shells select storage, compression, client, and server capabilities
explicitly. The Axum routes, WebSocket serving, external JWT/JWK verification,
catalogue HTTP orchestration, server state/builder, embedded server harness,
and legacy loopback listeners now live in `jazz-server`. Jazz exposes an opaque
semantic runtime handle plus schema-conversion vocabulary; it no longer has
normal dependencies on Axum, reqwest, tower-http, or tungstenite. The final
lane step is collapsing the temporary semantic `client`/`server` feature
selection left inside Jazz.

Groove is also featureless by default. Canonical tests and native Jazz shells
select its existing RocksDB adapter explicitly, so checking the semantic
storage/query core no longer compiles RocksDB or its native C++ dependency.
The implementation remains at its established public path until a source-
compatible adapter/facade split can replace it without a dependency cycle.

### Lane 4: storage, compression, and telemetry adapters

Move RocksDB, compression implementations, and OTLP exporters to shell-selected
adapters. Verify native consumers reuse one identical core artifact.

### Lane 5: optional protocol crate experiment

Attempt only after earlier measurements. Low-level protocol/schema/query changes
fan out to every consumer, so this boundary has the weakest prior and must
demonstrate an additional 50% win.

## Stop conditions

Stop when a slice creates more feature variants than it removes, requires broad
visibility expansion, or makes compatibility ownership less clear. Retain
independent improvements such as testkit separation, cache normalization, or
server/CLI boundaries when useful alone.

## GitHub outage publication queue (2026-08-17)

Publish the local work as one native stack when GitHub is available again.
Every adapter migration is atomic: move the implementation, migrate all
first-party composition, and delete the former path/feature in the same PR.
Do not publish compatibility-forwarding or duplicated-implementation steps.

1. `refactor: extract RocksDB storage adapter`
   - Base: `codex/jazz-core-engine-swap` after PR #1628.
   - Owns the erased ordered-KV wrapper/factory, `jazz-storage-rocksdb`, all
     first-party storage injection, and removal of RocksDB from Jazz/Groove.
2. `refactor: complete native transport composition`
   - Base: RocksDB extraction PR.
   - Migrates all native callers and the HTTP integration-test harness to
     explicit connector injection, then deletes Jazz's compatibility WebSocket
     implementation, re-export, and transport feature/dependencies. The test
     harness must not be used as a reason to retain a second socket adapter in
     the semantic crate.
3. `refactor: define server runtime facade`
   - Base: native transport PR.
   - Introduces only the narrow semantic operations required by an outward
     server shell; it does not widen Node/Db internals.
4. `refactor: move server implementation to jazz-server`
   - Base: server-facade PR.
   - Moves Axum routes, WebSocket serving, external JWT verification, JWK/HTTP
     work, and server state orchestration, then deletes the Jazz-owned paths.
5. `refactor: collapse semantic-core features`
   - Base: server extraction PR.
   - Removes obsolete client/server/embedded/test and adapter-forwarding
     features, establishes the canonical featureless build matrix, and records
     the stop/reassessment decision.
