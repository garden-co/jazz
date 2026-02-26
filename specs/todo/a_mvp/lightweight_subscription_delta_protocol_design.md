# Lightweight Subscription Delta Protocol (WASM <-> JS) — Design

## Overview

Current subscription deltas are heavy because they repeatedly send full row payloads across the boundary:

- `removed` sends full rows even when JS only needs `id` and/or `index`
- `updated` sends both `old` and `new` rows, duplicating data
- move-only updates still ship row payloads

This design introduces a compact delta protocol that preserves deterministic ordering while minimizing bytes and serialization work.

### Goals

- Reduce payload size and serde/JSON overhead between Rust and JS.
- Preserve correct `ORDER BY`, `LIMIT`, and `OFFSET` behavior.
- Keep JS application logic deterministic without full rescan.
- Support WASM, RN, and NAPI with one shared wire contract.

### Non-goals

- Changing query semantics.
- Replacing existing query engine internals.
- Introducing binary transport in this change (can be follow-up).

## Architecture / Components

### 1) New Wire Contract: `RowDelta`

Replace heavy entries with operation-oriented entries keyed by stable `id` and positions.

```ts
export interface RowDelta {
  protocolVersion: 2;
  pending: boolean;

  // Insert brand-new visible rows
  added: Array<{
    id: string;
    index: number; // index in post-window
    row: WasmRow; // full row only when entering window
  }>;

  // Remove rows leaving visibility
  removed: Array<{
    id: string;
    index: number; // index in pre-window
  }>;

  // Update existing visible rows
  updated: Array<{
    id: string;
    oldIndex: number; // index in pre-window
    newIndex: number; // index in post-window
    row?: WasmRow; // present only if row values changed
  }>;
}
```

Key reduction: `updated` no longer carries `old` row; `removed` no longer carries full row.

### 2) Rust-side Delta Classification

At binding serialization time (WASM/RN/NAPI), classify row updates into:

- **move-only**: same row content, only index changed -> `row` omitted
- **content-change**: content changed (with or without move) -> include `row`

Required helper behavior:

- maintain per-subscription `current_ids` for pre/post window index computation
- compute `oldIndex` and `newIndex`
- detect content change by comparing encoded row bytes (`old.data != new.data`) and commit id when needed
- for same-id updates, do not detach/re-append the id while deriving post-window indices
- implement index reconstruction once in `jazz-tools` (`query_manager::types::index_row_delta`) and reuse it in WASM/RN/NAPI bindings

### 3) Shared Serializer Shape Across Bindings

Apply identical JSON contract from:

- `crates/jazz-wasm`
- `crates/jazz-rn`
- `crates/jazz-napi`

This avoids client-side forks and keeps TS runtime binding-agnostic.

### 4) TypeScript Delta Application Model

`SubscriptionManager` consumes `RowDelta` with this flow:

1. Apply `removed` by `id` and `index`.
2. Apply `updated`:
   - reposition using indices
   - patch value only when `row` exists
3. Apply `added` by `index`.

JS keeps one `orderedIds` list and one `Map<id, item>`.

### 5) Fallback / Safety Path

No fallback full-reset payload is emitted in this protocol version.

## Data Models

### Rust (binding-level serialization structs)

```rust
struct Added {
    id: String,
    index: usize,
    row: WireRow,
}

struct Removed {
    id: String,
    index: usize,
}

struct Updated {
    id: String,
    old_index: usize,
    new_index: usize,
    row: Option<WireRow>,
}

struct RowDelta {
    protocol_version: u8, // = 2
    pending: bool,
    added: Vec<Added>,
    removed: Vec<Removed>,
    updated: Vec<Updated>,
}
```

### TypeScript runtime types

```ts
export type RowDelta = JazzWasmRowDeltaV2;
```

Compatibility policy:

- same release updates all bindings and TS runtime
- no dual-parser fallback in MVP scope

## Testing Strategy

### Unit Tests (Rust bindings)

- serializer emits `removed` entries without row payloads
- move-only updates emit `updated.row = None`
- content updates emit `updated.row = Some(...)`
- indices are pre/post window correct for add/remove/move
- same-id content-only updates preserve index unless changed by other operations in the batch

### Unit Tests (TS `subscription-manager`)

- move-only update reorders item without re-transforming row data
- content-only update updates item with same index
- move+content update handles both correctly
- mixed `added + removed + updated` in one delta remains deterministic
- `replaceAll` resets state correctly

### Integration Tests

- `ORDER BY + LIMIT` top-N churn: insert enters window, tail exits
- update changing sort key: row moves and optionally changes content
- remove from middle with subsequent index shifts
- sequence of deltas across multiple ticks matches full query result
- same behavior verified through WASM, RN, and NAPI subscription paths

Representative integration assertion pattern:

```ts
// After applying lite deltas in order, client state must match query snapshot
expect(deltaAppliedState).toEqual(await db.all(query));
```

## ASCII Scenarios

Legend:

- `A B C` are row ids in the visible result window.
- Numbers in parentheses are indexes.
- `row?` means payload is included only when content changed.

### 1) Record moved from one index to another (move-only)

Before:
`(0)A (1)B (2)C (3)D`

Event:
`C` sort key changes so it moves to the front, but row content is unchanged for projection.

Delta:

```json
{
  "updated": [{ "id": "C", "oldIndex": 2, "newIndex": 0 }],
  "added": [],
  "removed": []
}
```

Apply:

1. Remove id `C` from index 2
2. Insert id `C` at index 0

After:
`(0)C (1)A (2)B (3)D`

---

### 2) Removal that causes index shift

Before:
`(0)A (1)B (2)C (3)D`

Event:
`B` leaves visibility (deleted or filter-out).

Delta:

```json
{
  "removed": [{ "id": "B", "index": 1 }],
  "updated": [],
  "added": []
}
```

Apply:

1. Remove id `B` at index 1
2. Remaining ids naturally shift left

After:
`(0)A (1)C (2)D`

No extra payload is required for shifted records.

---

### 3) Mixed changes in one tick (remove + move/update + add)

Before:
`(0)A (1)B (2)C (3)D`

Event batch:

- `B` removed
- `D` content changes and moves from 3 -> 1
- `E` enters window at tail

Delta:

```json
{
  "removed": [
    { "id": "B", "index": 1 }
  ],
  "updated": [
    { "id": "D", "oldIndex": 3, "newIndex": 1, "row": { "...new values..." } }
  ],
  "added": [
    { "id": "E", "index": 3, "row": { "...values..." } }
  ]
}
```

Deterministic apply order:

1. Apply `removed` (pre-window indexes)
2. Apply `updated` moves/patches (`oldIndex` -> `newIndex`)
3. Apply `added` (post-window indexes)

After:
`(0)A (1)D (2)C (3)E`

This order ensures index references remain valid and reproducible.

## Rollout Plan

1. Implement protocol + serializers in WASM, RN, NAPI.
2. Update TS runtime types and `SubscriptionManager`.
3. Update orchestrator/integration tests to assert deterministic ordering.
4. Remove legacy heavy-delta assumptions in tests.

## Risks and Mitigations

- Risk: index drift across batched mixed operations.
  - Mitigation: strict pre/post index invariants + integration tests with mixed deltas.

- Risk: hidden binding divergence.
  - Mitigation: shared contract tests using JSON snapshots per binding.

## Success Metrics

- 40%+ reduction in avg subscription delta bytes for update-heavy workloads.
- Reduced JS callback parse/apply time in subscription benchmarks.
- No ordering regressions in existing subscription integration suites.

## Open Assumptions

- The runtime can safely depend on stable row `id` as canonical identity for repositioning.
- Field-level patches are out of scope; optional `row` replacement is sufficient for MVP.
- JSON transport remains acceptable after payload compaction; binary framing is follow-up.
