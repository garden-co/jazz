# Spatial point schema compatibility

This note records the compatibility contract for the new logical `Point`
schema type and the current S2-derived index work. Point is a new schema type;
there are no existing Point columns or array-to-Point conversions to migrate.
Spatial query predicates and a bounded root-read path are implemented, but the
feature is not release-ready yet. The capability remains unadvertised until
binding, relationship, subscription, and compatibility acceptance checks pass.

## Logical type and value bytes

`Point` is a public schema type for WGS84 coordinates in
`[longitude, latitude]` order. It is distinct from an ordinary array in schema
JSON and structural hashing, while its current-row value uses Groove's existing
`Array<F64>` representation. A non-null value is exactly two little-endian
IEEE-754 `f64` values (16 bytes); nullability uses the existing nullable
column envelope. This reuses the existing array value encoding and introduces
no new row-value byte layout.

Point values are validated at schema-default admission and every node cell
admission: the pair must have exactly two finite coordinates, longitude in
`[-180, 180]`, and latitude in `[-90, 90]`. Ordinary arrays remain ordinary
arrays and receive no point validation or spatial semantics.

## Schema identity

Schemas without point columns retain their existing canonical schema bytes and
IDs. A schema containing a point uses the domain
`jazz-schema-v3-spatial-points`; after each column's large-value semantic tag,
the encoding adds one byte identifying whether that column has point
semantics. Existing v1/v2 body fields remain in their established order. This
separates point columns from physically identical ordinary arrays without
changing the identity of existing non-spatial schemas.

The byte fixture is pinned in
`crates/jazz/layers/model/src/schema.rs` by
`point_schema_identity_uses_the_versioned_spatial_domain`.

The public type's JSON tag is separately pinned as `{"type":"Point"}` by
`point_has_a_distinct_public_schema_json_representation`. Point-bearing schema
publication and catalogue snapshots require `FEATURE_SPATIAL_POINTS` (`1<<10`);
an encoder rejects them when the peer did not negotiate that feature. Current
peers do not advertise the bit yet, so Point schemas and spatial query messages
remain unavailable over negotiated links until execution, binding, and
compatibility acceptance passes. Peers that do not advertise the bit cannot
receive Point-bearing messages.

The server's durable catalogue payload codec appends the Point column-type tag
as `17`, leaving every existing column-type tag unchanged; a round-trip byte
fixture pins this representation.

## S2 dependency acceptance gate

The adapter dependency is pinned as `s2rst = "=0.4.0"` behind the
Groove-owned `spatial` module. The upstream project labels itself beta, so the
specific operations used by Jazz require native and WASM compilation plus
independent S2 reference fixtures before release. The adapter uses full
polygon topology validation (`find_validation_error`) and separately detects
points on edges so `within` and `intersects` can keep their different boundary
contracts.

The pinned Rust 1.93.1 native build and the adapter's ten direct geometry tests
pass. The supported `jazz-wasm` package target also compiles for
`wasm32-unknown-unknown`. The adapter explicitly rejects antipodal edges
because `s2rst`'s loop validator accepts them. No planar fallback is used.

## Derived index and current execution envelope

The durable secondary-key contract is `groove.s2-point-key.v1`. Its contract
name is included in derived index definition bytes, while ordinary index
definition bytes retain their existing layout. Point sources use an atomic
`Array<F64>` value; the S2 `u64` cell is derived only for index keys and is
never exposed as a covered Point value. Runtime index maintenance uses the
same derivation for inserted, moved, and nulled rows.

Spatial predicate wire variants, range-based current Global and Local reads,
pending-row winner masking, and capability detection for registered spatial
query predicates are implemented. The TypeScript adapter translates all three
operators. Root reads apply S2 candidate access followed by the exact predicate;
Local reads merge pending index ranges and mask settled candidates using
targeted row-presence checks. Positive area OR branches across different
Point columns on one source union their conservative candidate ranges before
candidate reads; OR with an unanchored branch is rejected. `near` ranks by
spherical distance and stable row identity. Root collector lowering preserves
the nearest-first metadata, and maintained results retain that ordering. Node integration tests verify ordinary read
policies, synced Global changes entering and leaving a maintained area result,
no terminal updates for unrelated outside changes, and Local pending moves
masking old settled matches while adding new candidates.

The normalized access-path planner also permits a joined spatial query when
every stored source is independently bounded by an admitted index, or when
the outer root is fixed by its primary key and the related source has a spatial
index path. A maintained-query integration test covers the two-spatial-source
case and verifies that a related point leaving the area removes its owner.
Unbounded outer sources still fail before storage probes; dynamic lookup from
related candidates to all matching outer records remains unsupported.

The execution envelope remains narrower than the requested release. Spatial
filters may participate in row-set joins only when every stored input has its
own admitted index path; unbounded joins fail with `UnsupportedSpatialQuery`
before source access. Public relation-union arms preserve occurrence labels
when each arm independently has positive spatial bounds for every stored
source. Shared-source ranges are merged before reads, while an unbounded arm
fails before access; `near` inside a relation-union arm is rejected because
the arms would compete for nearest-first order. Positive area OR across Point
columns has focused candidate-deduplication coverage. Recursive relations,
NOT-only shapes, and OR branches without a positive bounded anchor also fail
before execution. Root spatial subscriptions have focused coverage
for synced point moves entering and leaving a polygon, unrelated out-of-range
changes, and Local pending moves into and out of the polygon. Related-only
spatial filters still need bounded relationship-key lookups to find outer rows.
Comprehensive permission/sync acceptance, restart/backfill and schema-variant
coverage, and React Native parity still need work. The
`FEATURE_SPATIAL_POINTS` bit is required by Point schema publications and
registered spatial query shapes, but remains out of the default advertised
feature set until the release gates pass. Unsupported relationship-side shapes
remain locally rejected before message emission. There is no migration
requirement for this new feature, and no array is silently assigned point
semantics.

## Verification status

Focused checks pass for Point schema conversion and identity, the S2 geometry
adapter, derived-index maintenance, the Jazz root read path (`near`/`within`/
`intersects`, including nearest ties, pending local moves, and bounded spatial
OR, pending deletion masking, and no source-wide fallback), deep `near` scope
validation, query-wide pre-compilation vertex admission, different-column area
OR candidate deduplication, and bounded relation-union behavior (including
distinct area ranges, retained arm occurrences, and zero-probe rejection of an
unbounded arm). Focused maintained-view ordering and row-permission checks,
spatial wire gating, Jazz Model's 251 library tests, TypeScript query-adapter
tests (43), DSL/value/native-row/WebSocket tests (145), TypeScript test-project
typechecking, and both
`jazz-wasm --target wasm32-unknown-unknown` and `jazz-napi` compile checks.

This turn's focused regression checks pass: `groove`'s ten geometry tests,
Jazz Model's recursive `near`-scope and query-wide vertex-budget tests, and all
eight node spatial tests. The latter cover different-column OR deduplication,
bounded relation-union arms over distinct areas, retained public occurrences,
and zero-probe rejection of an unbounded arm. `cargo fmt --check` and
`git diff --check` also pass.

The lint partition's format, Oxlint/Clippy, Jazz Tools typecheck, and crate
publication phases passed. Its CI workflow contract phase did not: the run
reported npm `EPERM` writes under `/Users/carlosbensant/.npm`, correctness
artifact source-manifest mismatches, and artifact-lock holder/subprocess
failures. It was manually stopped after the artifact-pipeline test remained
active for 461.8 seconds; the partition therefore failed and is not counted as
passing. The official correctness-artifact producer was then retried with a
task-local lock, npm cache, and temporary directory. NAPI compilation reached
linking but failed with `ld: write() failed, errno=28` when the volume had only
125 MiB free, so no producer receipt was accepted. The `--ci-equivalent` gate
was not run. React Native spatial parity remains unverified, and the
`packages/jazz-wasm` package required by that target is absent. The spatial
capability remains out of the default advertised feature set until these
release checks pass.
