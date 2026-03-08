# Durability Tier API Unification (Breaking, MVP)

## Status

Proposed and approved for immediate implementation.

## Goals

1. Replace durability terminology with a single, intuitive model.
2. Remove split read/write tier naming from public APIs.
3. Make base CRUD/query APIs tier-aware (no suffixed tier methods).
4. Preserve local-first UX while supporting tier-gated reads.
5. Support nodes that represent multiple durability identities.

## Non-Goals

1. Consistency/merge/transaction semantics (handled separately).
2. Backward compatibility shims or aliases.

## Terminology

`DurabilityTier` replaces all prior tier naming:

1. `worker`
2. `edge`
3. `global` (renamed from `core`)

Ordering: `worker < edge < global`.

## Public API (TypeScript)

### Types

```ts
export type DurabilityTier = "worker" | "edge" | "global";

export type ReadOptions = {
  tier?: DurabilityTier;
  localUpdates?: "immediate" | "deferred";
  propagation?: "full" | "local-only";
};

export type WriteOptions = {
  tier?: DurabilityTier;
};
```

Default read behavior:

1. `localUpdates` defaults to `"immediate"`.
2. Read `tier` default is environment-specific:
   1. client: `"worker"`
   2. backend: `"edge"`

Default write behavior (`WriteOptions.tier` omitted):

1. client: `"worker"`
2. backend: `"edge"`

### Method shape (sync base + durable suffix variants)

Base mutation methods stay synchronous and local-first:

1. `insert(...): Row`
2. `update(...): void`
3. `delete(...): void`

Durable variants opt into acknowledgement promises:

1. `insertDurable(..., options: WriteOptions): Promise<Row>`
2. `updateDurable(..., options?: WriteOptions): Promise<void>`
3. `deleteDurable(..., options?: WriteOptions): Promise<void>`

Read methods continue to accept `ReadOptions`:

1. `all(..., options?: ReadOptions): Promise<...>`
2. `one(..., options?: ReadOptions): Promise<...>`
3. `subscribeAll(..., options?: ReadOptions): () => void`
4. Client-level `query/subscribe` equivalents take `ReadOptions`.

Removed APIs:

1. `*WithAck`
2. `*Persisted`
3. `settledTier` option naming
4. any read/write-suffixed tier-specific variant names
5. `PersistenceTier`

## Read semantics

`ReadOptions.tier` gates durability confirmation for initial read/subscription settlement.

`localUpdates` controls delivery of updates caused by local writes while durability gating is active:

1. `"immediate"` (default): the initial delivery still waits for `ReadOptions.tier`; after that first settled snapshot, local write-driven subscription updates deliver synchronously.
2. `"deferred"`: preserve strict tier gating for both initial and subsequent deliveries.

## Runtime semantics

## Multi-tier identities

A node may advertise multiple durability identities (not a single optional tier).

Examples:

1. `jazz-tools` CLI server: `["edge", "global"]`
2. current cloud-server deployment: `["edge", "global"]`

Behavior:

1. Nodes emit `PersistenceAck` for every identity they represent.
2. Nodes emit `QuerySettled` for every identity they represent.
3. Emission order is deterministic and ascending by durability rank (`edge` then `global` for dual-identity servers).

## Implementation plan (required changes)

1. Replace core tier type/string parsing across Rust/TS bindings:
   1. `core` -> `global`
   2. `PersistenceTier` -> `DurabilityTier`
2. Change runtime/sync manager configuration from single tier to multi-identity set.
3. Update ack/settlement emission to emit for each identity.
4. Replace read option names and behavior wiring:
   1. `settledTier` -> `tier`
   2. add `localUpdates`
5. Replace base mutation APIs to accept `WriteOptions` and apply defaults by environment.
6. Remove legacy suffixed APIs and all usage across packages/examples/docs/tests.
7. Update docs language:
   1. “write ack tier” / “read settled tier” -> durability tier semantics
   2. `core` -> `global`

## Tests and docs updates

All affected tests and docs must be updated in the same change:

1. runtime core durability/settlement tests
2. wasm/napi/nitro tier parse and API tests
3. browser worker-bridge and subscription behavior tests
4. backend context defaults tests
5. quickstarts and API docs pages
6. examples snippets

No compatibility layer is allowed.
