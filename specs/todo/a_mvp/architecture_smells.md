# Architecture Smells (Deep Audit)

This document captures the highest-impact architecture smells found during a deep audit focused on elegance and minimalism for a greenfield-friendly codebase.

## 1) Parallel query contracts (legacy query shape + relation IR)

The system currently maintains both a legacy query payload shape and relation IR as wire inputs. This creates translation and normalization layers that drift over time and increase complexity.

- Evidence:
  - `packages/jazz-tools/src/runtime/query-adapter.ts`
  - `crates/jazz-tools/src/query_manager/query_wire.rs`
  - `crates/jazz-tools/src/query_manager/query.rs`
- Why it smells:
  - Two sources of truth for one concept.
  - Extra compatibility code paths inside critical query parsing.
- Direction:
  - Move to relation-IR-only wire contract.
  - Remove compat normalizers and legacy query-shape emission.

## 2) Silent subscription recompile failures

When schema-driven recompilation fails, subscriptions can be marked as if recompilation is complete.

- Evidence: `crates/jazz-tools/src/query_manager/manager.rs`
- Why it smells:
  - Hidden stale state.
- Direction:
  - Preserve failure state and retry/surface errors explicitly.

## 3) Intentional index staleness fallback

Update paths tolerate stale indexing when old row content is missing.

- Evidence: `crates/jazz-tools/src/query_manager/manager.rs`
- Why it smells:
  - Query correctness becomes probabilistic under some sync histories.
- Direction:
  - Replace with explicit reindex/recovery workflow.

## 4) Lens transform failures degrade silently

Failed lens transforms can fall back to original data and continue.

- Evidence: `crates/jazz-tools/src/query_manager/manager.rs`
- Why it smells:
  - Schema mismatch can be silently propagated.
- Direction:
  - Fail closed for that row/subscription and surface deterministic errors.

## 5) Duplicated sync transport state machines

Main-thread client and worker each implement similar reconnect/auth/streaming logic.

- Evidence:
  - `packages/jazz-tools/src/runtime/client.ts`
  - `packages/jazz-tools/src/worker/jazz-worker.ts`
- Why it smells:
  - Divergence risk and duplicated bug-fix cost.
- Direction:
  - Consolidate into a shared sync engine/state machine.

## 6) Deprecated API aliases still retained despite no-backcompat stance

Deprecated persisted mutation aliases remain in runtime/db APIs.

- Evidence:
  - `packages/jazz-tools/src/runtime/client.ts`
  - `packages/jazz-tools/src/runtime/db.ts`
- Why it smells:
  - Surface-area bloat.
- Direction:
  - Remove aliases and keep one canonical mutation API.

## 7) Thin but duplicated React / React Native wrappers

Multiple wrapper layers expose near-identical provider/client/useAll structures.

- Evidence:
  - `packages/jazz-tools/src/react/*`
  - `packages/jazz-tools/src/react-native/*`
  - `packages/jazz-tools/src/react-core/*`
- Why it smells:
  - Repeated adapter seams for similar logic.
- Direction:
  - Single UI adapter with minimal platform shims.

## 8) Per-tick full schema map cloning

Schema manager copies known schema maps into query manager during processing.

- Evidence: `crates/jazz-tools/src/schema_manager/manager.rs`
- Why it smells:
  - O(n) churn in hot flow.
- Direction:
  - Incremental updates or shared immutable snapshots.

## 9) Storage backend key-layout duplication

`opfs_btree` and `surrealkv` repeat key encoding and namespace logic.

- Evidence:
  - `crates/jazz-tools/src/storage/opfs_btree.rs`
  - `crates/jazz-tools/src/storage/surrealkv.rs`
- Why it smells:
  - Parallel maintenance burden.
- Direction:
  - Shared key codec/helpers used by all storage implementations.

## 10) Over-broad module exports and monolithic files

Large modules and broad re-exports reduce boundary clarity.

- Evidence:
  - `crates/jazz-tools/src/query_manager/mod.rs`
  - `crates/jazz-tools/src/runtime_core.rs`
- Why it smells:
  - Higher accidental coupling and harder reasoning.
- Direction:
  - Narrow exports and split oversized modules by responsibility.

---

## Immediate Priority

1. Collapse query wire contract to relation-IR-only.
2. Unify sync transport state machine.
3. Remove no-backcompat legacy aliases.
