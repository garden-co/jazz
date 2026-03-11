# Mutation Outcome And Rejection Infrastructure (Rust-First, MVP)

## Status

Proposed for immediate implementation.

## Goals

1. Add shared Rust-core infrastructure for tracking local mutations after they leave the local-first fast path.
2. Distinguish durability from authoritative sync outcome.
3. Persist mutation outcome state so rejections survive restart and long offline periods.
4. Make rejection handling available to all bindings through core event and lookup APIs rather than TS-only logic.
5. Roll back rejected local mutations in core so the canonical local database converges back to accepted state.

## Non-Goals

1. Defining app UX defaults in core APIs.
2. Shipping a TS-specific issue inbox abstraction.
3. Storing app-specific draft payloads or recovery UI state in core storage.
4. Solving multi-row transactions or cross-object atomic outcome tracking.
5. Reworking read durability semantics; this RFC is write-focused.

## Problem

Current behavior is insufficient in four ways:

1. The server already emits permission denials as `SyncPayload::Error(SyncError::PermissionDenied { ... })`, but the client path mostly logs or ignores them.
2. There is no persisted mutation ledger, so a rejection that arrives hours later cannot be surfaced reliably after restart.
3. There is no core rollback path for rejected optimistic writes, so rejected local commits can remain visible indefinitely.
4. Durable mutation waiters only understand `PersistenceAck`; a mutation rejected before the requested remote ack can hang forever.

This is fundamentally a Rust-core concern, not a frontend concern. Bindings should consume shared mutation outcome state rather than each inventing separate rejection tracking.

## Terminology

### Durability

Whether a commit has been persisted at a tier (`worker < edge < global`).

This remains modeled by `PersistenceAck`.

### Outcome

Whether the first authoritative sync server has accepted or rejected a local mutation.

Outcome is not the same thing as durability:

1. A mutation may be durable at `worker` and still later be rejected remotely.
2. A mutation may be accepted by the authority but not yet durable at the caller's requested tier.

### Mutation Record

A persisted local record describing one local write operation and its lifecycle.

### Rejection

An authoritative negative outcome for a local mutation, including the reason and the commits that must be rolled back.

## Design Summary

The MVP introduces four pieces:

1. A persisted mutation journal in Rust storage.
2. An explicit sync protocol payload for mutation outcome (`accepted` or `rejected`).
3. RuntimeCore watchers and event queues that bindings can expose as callbacks/promises/lookups.
4. Core rollback of rejected local commit chains.
5. An explicit acknowledgement step that bounds retained rejected outcomes and prunes dead local commits.

The core rule is:

`PersistenceAck` answers "is this durable at tier T?"

`MutationOutcome` answers "did the authority accept or reject this write?"

Both are required.

The retention rule is:

1. `pending` outcomes are retained while unresolved
2. `rejected` outcomes are retained until acknowledged by the app
3. `accepted` outcomes may be compacted aggressively and do not require user acknowledgement
4. unacknowledged rejected outcomes are capped by a high count bound to avoid unbounded storage growth if the app never acknowledges them

## Proposed Rust Types

### Identifiers And Basic Enums

```rust
pub struct MutationId(pub Uuid);

pub enum MutationOperation {
    Insert,
    Update,
    Delete,
}

pub enum MutationRejectCode {
    PermissionDenied,
    SessionRequired,
    CatalogueWriteDenied,
}
```

### Outcome Payloads

```rust
pub enum MutationOutcome {
    Accepted(MutationAcceptance),
    Rejected(MutationRejection),
}

pub struct MutationAcceptance {
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub operation: MutationOperation,
    pub commit_ids: Vec<CommitId>,
    pub previous_commit_ids: Vec<CommitId>,
    pub accepted_at_micros: u64,
}

pub struct MutationRejection {
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub operation: MutationOperation,
    pub commit_ids: Vec<CommitId>,
    pub previous_commit_ids: Vec<CommitId>,
    pub code: MutationRejectCode,
    pub reason: String,
    pub rejected_at_micros: u64,
}
```

Why commit IDs, not protocol-level mutation IDs:

1. Commit IDs already exist at mutation creation time.
2. Server-side permission evaluation already has access to the rejected payload and therefore its commit IDs.
3. This avoids adding client-only correlation fields to `ObjectUpdated`.

Local `MutationId` still exists in storage and lookup APIs, but wire correlation uses commit IDs in MVP.

### Persisted Mutation Journal

```rust
pub enum MutationOutcomeState {
    Pending,
    Accepted,
    Rejected(MutationRejection),
    SupersededByRejection { root_mutation_id: MutationId },
}

pub struct MutationRecord {
    pub id: MutationId,
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub table: Option<String>,
    pub operation: MutationOperation,
    pub commit_ids: Vec<CommitId>,
    pub previous_commit_ids: Vec<CommitId>,
    pub recorded_at_micros: u64,
    pub highest_acked_tier: Option<DurabilityTier>,
    pub outcome: MutationOutcomeState,
}
```

`highest_acked_tier` and `outcome` are intentionally separate fields.

Rejected mutation records are durable until explicitly acknowledged. Accepted records are allowed to be compacted after they have served event/overlay consumers.

Wall-clock timestamps are part of the MVP. Use microseconds since Unix epoch for consistency with existing commit timestamps and ordering.

### Object Outcome Overlay

Bindings need a row-visible outcome surface that does not require a separate TS-only issue store.

Core should therefore also expose an object-scoped overlay:

```rust
pub enum ObjectOutcomeState {
    Pending {
        mutation_id: MutationId,
    },
    Accepted {
        mutation_id: MutationId,
    },
    Errored {
        mutation_id: MutationId,
        code: MutationRejectCode,
        reason: String,
    },
}
```

This is derived state, not a second journal.

Rules:

1. It is keyed by `object_id`.
2. It represents the latest locally relevant outcome that has not yet been compacted away.
3. `Errored` remains until acknowledgement.
4. `Accepted` is transient and may disappear after bindings have had a chance to surface it.

### Runtime Events

```rust
pub enum MutationEvent {
    Recorded { mutation_id: MutationId },
    AckAdvanced {
        mutation_id: MutationId,
        tier: DurabilityTier,
    },
    Accepted {
        mutation_id: MutationId,
    },
    Rejected {
        mutation_id: MutationId,
        rejection: MutationRejection,
    },
    SupersededByRejection {
        mutation_id: MutationId,
        root_mutation_id: MutationId,
    },
    Acknowledged {
        mutation_id: MutationId,
    },
}
```

Bindings can turn these into callbacks, observables, or event emitters. Core only needs a drainable queue.

## Sync Protocol Changes

### New Payload Variant

`SyncPayload` gains:

```rust
SyncPayload::MutationOutcome(MutationOutcome)
```

`SyncPayload::Error` remains for non-mutation protocol failures such as:

1. `QuerySubscriptionRejected`
2. general sync/server diagnostics

Mutation denials stop using generic `SyncPayload::Error` and instead use `SyncPayload::MutationOutcome(MutationOutcome::Rejected(...))`.

### Emission Rules

The first authoritative server that processes a client-originated write must send one terminal outcome back to the originating client:

1. `Accepted` after the write is successfully applied server-side.
2. `Rejected` if the write is denied before apply.

This includes:

1. User writes denied by ReBAC.
2. Immediate write denials such as `SessionRequired` or `CatalogueWriteDenied`.
3. Trusted backend/admin writes that are accepted without ReBAC still emit `Accepted`.

Peer/server-to-server relays do not need local mutation outcome tracking.

Only the first authority emits acceptance or rejection for a given client-originated mutation. Upstream authorities do not re-emit accepted outcomes for the same mutation.

### Ordering

`MutationOutcome` is terminal and single-shot per source mutation.

Invariants:

1. A mutation cannot emit both `Accepted` and `Rejected`.
2. If a parent commit is rejected, a descendant mutation from the same local chain must not later be accepted.
3. `PersistenceAck` may arrive before or after `Accepted`, depending on topology, but `Rejected` forbids any later acceptance.

## Core State Ownership

### SyncManager

`SyncManager` should gain:

1. `received_mutation_outcomes: Vec<MutationOutcome>`
2. helper methods for emitting `MutationOutcome` to a source client

Responsibilities:

1. Turn server-side write results into `MutationOutcome` payloads.
2. Queue inbound `MutationOutcome` messages for `RuntimeCore`.

### RuntimeCore

`RuntimeCore` should gain:

1. a persisted mutation journal facade backed by `Storage`
2. mutation waiters keyed by local `MutationId`
3. a drainable `mutation_events` queue
4. object outcome overlay lookup/invalidation

Suggested fields:

```rust
mutation_events: Vec<MutationEvent>,
mutation_waiters: HashMap<MutationId, Vec<oneshot::Sender<Result<(), MutationRejection>>>>,
commit_to_mutation: HashMap<CommitId, MutationId>,
```

`commit_to_mutation` may be reconstructed from storage on startup or lazily hydrated.

### Storage

`Storage` should gain first-class mutation journal methods.

Suggested trait additions:

```rust
fn put_mutation_record(&mut self, record: MutationRecord) -> Result<(), StorageError>;

fn load_mutation_record(
    &self,
    mutation_id: MutationId,
) -> Result<Option<MutationRecord>, StorageError>;

fn load_mutation_record_by_commit(
    &self,
    commit_id: CommitId,
) -> Result<Option<MutationRecord>, StorageError>;

fn delete_mutation_record(
    &mut self,
    mutation_id: MutationId,
) -> Result<(), StorageError>;

fn list_mutation_records_by_outcome(
    &self,
    outcome: MutationOutcomeFilter,
) -> Result<Vec<MutationRecord>, StorageError>;
```

The exact storage indexing can differ by backend. The important requirement is that mutation state survives restart and is queryable by commit ID and outcome state.

The journal does not need to retain acknowledged rejected records or compacted accepted records.

### Retention Bound

Add a hard safety bound for unacknowledged rejected outcomes:

```rust
const MAX_RETAINED_UNACKNOWLEDGED_REJECTIONS: usize = 10_000;
```

Scope:

1. per local app database / storage namespace
2. applies only to rejected outcomes awaiting acknowledgement

When the bound is exceeded:

1. the oldest rejected outcomes by `rejected_at_micros` are force-compacted oldest-first
2. their dead/local-unreachable commit chains are pruned
3. their object outcome overlays are cleared

This is a storage safety valve, not the primary user-facing flow.

## Mutation Recording

Every local write recorded by RuntimeCore should create a `MutationRecord` immediately after the local commit is created.

This applies to:

1. `insert`
2. `update`
3. `delete`
4. durable variants of the above

Recording rules:

1. Generate a `MutationId`.
2. Capture `object_id`, `branch_name`, `operation`, `commit_ids`, `previous_commit_ids`, and table name if known.
3. Persist the record with `outcome = Pending`.
4. Emit `MutationEvent::Recorded`.

This must happen for both sync and durable mutation APIs so later lookup works uniformly.

If the write affects a currently visible object, the object outcome overlay should become `Pending` and active subscriptions for rows containing that object should be invalidated.

## Durable Mutation Waiter Semantics

Durable mutation APIs remain durability-based, but they must also understand rejection.

Rules:

1. A waiter for `edge` or `global` must reject if the mutation is authoritatively rejected before the requested ack arrives.
2. A waiter for `worker` may resolve before remote outcome. Later rejection is then surfaced only through mutation events and lookup APIs.
3. Durable waiters must never hang forever after a terminal rejection.

This preserves the meaning of durability tiers while fixing the current missing negative path.

## Acceptance Handling

On inbound `MutationOutcome::Accepted`:

1. Find local mutation record(s) by commit ID.
2. Mark outcome as `Accepted`.
3. Persist the update.
4. Emit `MutationEvent::Accepted`.

Acceptance does not resolve durability waiters by itself. Only `PersistenceAck` resolves durability thresholds.

If an object remains visible after acceptance, the object outcome overlay may briefly transition to `Accepted` before being compacted.

## Rejection Handling

On inbound `MutationOutcome::Rejected`:

1. Find the root local mutation record by rejected commit ID.
2. Compute all pending local descendant mutations that depend on the rejected commit chain.
3. Roll back the affected local commits from the canonical object graph.
4. Mark the root record `Rejected`.
5. Mark descendant local records `SupersededByRejection { root_mutation_id }`.
6. Persist all state updates.
7. Reject unresolved durable waiters for the root and descendants.
8. Emit `Rejected` and `SupersededByRejection` events.

The rejection payload itself should be persisted on the root mutation record.

## Rollback Semantics

Rollback is a correctness requirement, not just UX.

### Scope

Rollback is per object/branch mutation chain in MVP.

The affected set is:

1. the rejected mutation's commits
2. any still-pending local descendant mutations whose `previous_commit_ids` depend on that chain

### Restore Point

Restore tips are the nearest surviving ancestor commit IDs not included in the affected rejected/superseded set.

Examples:

1. rejected insert with no prior history: object disappears locally
2. rejected update after accepted commit `c1`: branch tip restores to `c1`
3. rejected delete after accepted commit `c7`: branch tip restores to `c7`

### Implementation Shape

The rollback operation should live below bindings, likely as a `QueryManager`/`ObjectManager` helper invoked by RuntimeCore. It must:

1. remove rejected local commit(s) from the active branch tips so they no longer affect canonical query results
2. restore prior tips
3. retain the rejected commit chain as locally dead/unreachable until acknowledgement
4. trigger the same query invalidation/update path as ordinary data changes

No app code should have to manually revert rejected writes.

This split is important:

1. rejection rollback is immediate for correctness
2. physical pruning is deferred until acknowledgement so the rejection can still be inspected and surfaced

## Acknowledgement And Pruning

Rejected outcomes must be bounded. The mechanism is explicit acknowledgement.

Core should expose:

```rust
pub fn acknowledge_mutation_outcome(
    &mut self,
    mutation_id: MutationId,
) -> Result<(), RuntimeError>;
```

Semantics:

1. The call is valid for rejected outcomes and idempotent for already-removed records.
2. It clears the unacknowledged rejected outcome from the durable journal.
3. It prunes the rejected dead/local-unreachable commit chain that was retained after rollback.
4. It clears any object outcome overlay derived from that rejected outcome.
5. It emits `MutationEvent::Acknowledged`.

This is the primitive both row-level and global notification surfaces should call.

Accepted outcomes do not require acknowledgement and should usually be compacted automatically.

If a rejected outcome is removed by the high count bound rather than explicit acknowledgement, core should perform the same prune-and-clear behavior without surfacing it as a user acknowledgement.

## Notification Model

Core should support two notification surfaces.

### Visible Data: Row Outcome Overlay

For rows that are currently visible through active queries, bindings should be able to expose a reserved `$outcome` field derived from `ObjectOutcomeState`.

Conceptually:

```ts
$outcome:
  | { type: "pending" }
  | { type: "accepted" }
  | { type: "errored", code, reason, acknowledge: () => void }
```

The closure is binding-created. Core only needs to provide the underlying `mutation_id`.

Important consequence:

Active subscriptions must re-fire when `$outcome` changes, even if the row's ordinary columns do not.

That means outcome overlay changes are part of the subscription invalidation model, not merely a passive lookup table.

### Invisible Data: Global Outcome Events

For mutations that no active query currently surfaces, bindings should expose a global hook/event stream.

Example shape at the binding layer:

```ts
onMutationOutcome((event) => {
  if (event.type === "rejected") {
    event.acknowledge();
  }
});
```

Again, the callback is binding-level sugar over `acknowledge_mutation_outcome(mutation_id)`.

### Rejected Inserts

A rejected insert often will not have any visible row after rollback, so the global outcome hook remains necessary even if row overlays exist.

## Lookup APIs

Core should expose shared lookup methods instead of embedding UI concepts.

Suggested RuntimeCore surface:

```rust
pub fn take_mutation_events(&mut self) -> Vec<MutationEvent>;

pub fn get_mutation_record(
    &self,
    mutation_id: MutationId,
) -> Result<Option<MutationRecord>, RuntimeError>;

pub fn get_mutation_record_by_commit(
    &self,
    commit_id: CommitId,
) -> Result<Option<MutationRecord>, RuntimeError>;

pub fn list_rejected_mutations(&self) -> Result<Vec<MutationRecord>, RuntimeError>;

pub fn list_pending_mutations(&self) -> Result<Vec<MutationRecord>, RuntimeError>;

pub fn list_mutations_for_object(
    &self,
    object_id: ObjectId,
) -> Result<Vec<MutationRecord>, RuntimeError>;

pub fn get_object_outcome(
    &self,
    object_id: ObjectId,
) -> Result<Option<ObjectOutcomeState>, RuntimeError>;

pub fn acknowledge_mutation_outcome(
    &mut self,
    mutation_id: MutationId,
) -> Result<(), RuntimeError>;
```

Bindings may wrap these as:

1. callbacks
2. event emitters
3. reactive stores
4. row enrichment with `$outcome`
5. promise rejection helpers

The Rust core should not define a TS-only inbox abstraction.

## Binding Guidance

Bindings should build language-appropriate APIs on top of core state:

1. callback-style: `onMutationOutcome(...)`
2. lookup-style: `listRejectedMutations()`, `getMutationRecord(...)`
3. row-level `$outcome` enrichment for visible data
4. global notifications for invisible data
5. durable API rejection for unresolved remote-tier waiters

Bindings should not duplicate mutation journals in binding-local storage.

## Documentation Guidance

Docs should recommend app patterns, but those patterns are not part of the Rust core contract.

Recommended topics for docs and examples:

1. show inline row-level state when the affected object is visible
2. also surface delayed rejections in a global sync issues area
3. explain that `worker` durability does not imply remote acceptance
4. recommend app-managed draft recovery if rejected payload content must be restorable
5. explain how `acknowledge()` clears a surfaced rejection and prunes retained dead commits
6. explain how to query mutation records by object ID after an update/delete call

This guidance should live in docs/examples, not in core types.

## Migration From Current Behavior

### SyncError

Move these out of generic mutation-error handling:

1. `PermissionDenied`
2. `SessionRequired`
3. `CatalogueWriteDenied`

They become `MutationRejectCode` values carried by `MutationOutcome::Rejected`.

`SyncError::QuerySubscriptionRejected` stays as-is.

### Durable APIs

No API rename is required for this RFC.

Behavior changes:

1. edge/global durable waiters reject on terminal rejection
2. worker durable waiters may still resolve before later rejection
3. visible rows can expose `$outcome` via shared core lookups/invalidation
4. rejected outcomes remain until acknowledged, then are pruned

## Implementation Plan

1. Add mutation types and `SyncPayload::MutationOutcome` in `sync_manager/types.rs`.
2. Emit accepted/rejected outcome messages in `sync_manager/permissions.rs` and direct-write paths in `sync_manager/inbox.rs`.
3. Add inbound outcome queue plumbing in `SyncManager`.
4. Extend `Storage` and all backends with mutation journal persistence.
5. Record local mutations in `RuntimeCore` write paths.
6. Add RuntimeCore mutation waiters, `take_mutation_events()`, and `acknowledge_mutation_outcome(...)`.
7. Implement rollback helper(s) in QueryManager/ObjectManager for rejected local commit chains, with deferred prune-on-acknowledge.
8. Invalidate subscriptions when object outcome overlays change.
9. Update bindings to consume core mutation events/lookups and expose `$outcome`/global hooks.
10. Add docs/examples describing recommended surfacing patterns.

## Tests Required

1. rejection persists across restart and remains queryable
2. rejected mutation rolls back local visible state
3. descendant pending mutations are superseded when an ancestor is rejected
4. `edge`/`global` durable waiters reject instead of hanging
5. `worker` durable waiters may resolve before later rejection, and rejection still appears in mutation events
6. accepted outcome marks mutation as accepted without resolving durability prematurely
7. immediate denials (`SessionRequired`, `CatalogueWriteDenied`) use `MutationOutcome::Rejected`
8. generic query-subscription errors still flow through `SyncError`
9. acknowledging a rejected outcome clears the overlay, removes the journal entry, and prunes the dead commit chain
10. subscriptions re-fire when `$outcome` changes even without ordinary row data changes

## Resolved Decisions

1. Mutation records and accepted/rejected outcomes include wall-clock timestamps in microseconds since Unix epoch.
2. Accepted/rejected outcomes are emitted only by the first authority for a client-originated mutation.
3. Rejected outcomes awaiting acknowledgement are capped by a high count bound of `10_000`, with oldest-first forced compaction as a storage safety valve.
