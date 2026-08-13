# Cleanup roadmap

This document records high-leverage cleanup work that should make Jazz easier
to understand, test, and change without altering its architectural invariants.
In particular, rows remain the unit of permissions and synchronization.

As a rough maintainability rule, implementation and test files should normally
stay below 2,000 lines. Exceeding that threshold is a prompt to identify real
module boundaries, not an invitation to split files arbitrarily.

## Recommended order

### 1. Split the largest Rust modules without changing behavior

Start with mechanical moves and preserve exact behavior. Likely boundaries:

- `query_eval.rs` (roughly 19,000 lines): normalization, source resolution,
  compilation, one-shot execution, maintained-subscription reduction, and
  tests.
- Groove `runtime/mod.rs` (roughly 14,000 lines): collectors, windows,
  aggregates, joins, and delta reconstruction.
- Query lowering (roughly 9,000 lines): projections, collectors, aggregates,
  policy/support routes, and terminal construction.
- Node `mod.rs` (roughly 7,000 lines): state, persistence, catalogue lifecycle,
  recovery, and cache coordination.
- `peer.rs` (roughly 7,000 lines): transport state, subscriptions, authority
  support, and delivery.
- `ingest.rs` (roughly 6,000 lines): admission, catalogue activation,
  snapshots, and parked work.

Mechanical extraction should land before semantic redesigns so later reviews
can distinguish moved code from changed behavior.

### 2. Split oversized TypeScript runtimes and tests

- Divide `runtime/native-runtime/runtime.test.ts` (roughly 8,000 lines) into
  codec, transaction, subscription, connection-lifecycle, policy, and
  persistence suites.
- Separate the persistent browser runtime's actor state, connection/server
  lifecycle, transaction bookkeeping, and serialization.
- Separate browser subscription ownership and lease management from `Db`
  forwarding.
- Split create-client coverage into registry/lifecycle, authentication,
  browser-mode, and framework-integration suites.
- Prefer shared contract suites that run against both NAPI and WASM over
  duplicated scenarios.

### 3. Remove dead and obsolete seams

- Remove the unused `ResultTransitions::observed_delta_batches` field.
- Remove or justify the unused singular `structured_app_row` hook.
- Remove stale compatibility comments that refer to already-landed PRs.
- Consolidate duplicated subscription-carrier normalization helpers.
- Remove production paths retained only for compatibility tests, or explicitly
  isolate and document them as compatibility boundaries.

### 4. Establish one NAPI/WASM binding codec contract

Proceed fixture-first:

1. Define one Rust-owned row, delta, and terminal-layout encoding contract.
2. Add golden fixtures consumed by Rust and TypeScript.
3. Reduce NAPI and WASM to thin host adapters over the shared contract.
4. Add fast exhaustive unit coverage before relying on browser integration
   tests.
5. Compile TypeScript decoders by registered layout ID, with a safe fallback.

This should remove duplicated binding logic and catch representation drift at a
fast, deterministic boundary.

### 5. Simplify maintained-subscription state

Result membership, payloads, authoritative state, optimistic state, and program
facts currently have several parallel owners. Introduce one indexed result-state
model with explicit provenance and derive convenience views from it.

This is correctness-sensitive: preserve optimistic replacement, authoritative
reset ordering, aggregate NULL behavior, and whole-row delivery. Do it after the
mechanical module splits make the state owners legible.

### 6. Unify terminal construction

Replace string-based sink conventions and separate aggregate/non-aggregate
builders with a typed `TerminalPlan` that owns:

- public versus internal fields;
- per-field carrier representation;
- route metadata;
- terminal layout IDs; and
- aggregate output namespaces.

The external sink-name encoding may remain stable while the compiler uses typed
identities internally.

### 7. Take the remaining CI and build-speed wins

- Use an immutable, provisioned runner tool bundle and an explicitly managed
  shared sccache daemon. CI jobs should validate and consume these tools, not
  reinstall or restart them.
- Cache pnpm dependencies across starter end-to-end jobs.
- Avoid rerunning Rust differential binaries already selected by the workspace
  test run.
- Add compiler caching for macOS and Windows release builds.
- Make focused Rust commands fail when they execute zero tests.
- Check generated-artifact provenance automatically at consumption time.
- Bound browser logs and stop quickly on repeated infrastructure failures.

### 8. Clarify terminology and ownership

Continue `JARGON_BURNDOWN.md` for terms such as route, terminal, carrier, fact,
witness, maintained, settled, and authoritative. Prefer better concepts and
abstractions over merely adding definitions.

Add and maintain a short representation-ownership map:

```text
query -> normalized query -> lowered graph -> terminal schema
      -> maintained state -> binding payload -> TypeScript decoder
```

## Suggested parallel tracks

The following can progress independently:

1. Mechanical module and test-file splitting toward the 2,000-line guideline.
2. Shared NAPI/WASM codec fixtures and contract extraction.
3. Small dead-code, focused-test, artifact-provenance, and CI-speed fixes.

Defer the catalogue state-machine and maintained-result-state redesigns until
the structural splits land. Otherwise those already-sensitive changes will be
unnecessarily difficult to review.
