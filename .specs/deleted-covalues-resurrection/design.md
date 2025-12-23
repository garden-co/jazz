# Design: Deleted coValues resurrection

## Overview

This design adds **resurrection** as a new lifecycle phase for coValues.

Resurrection lets users bring a deleted coValue back **without restoring old data**. Practically, this means:

- we keep the coValue **header** (type + ownership metadata),
- we keep lifecycle event markers (delete / resurrection),
- but we treat the content history as partitioned into “lives”, and only the **active life** is considered when loading/syncing/applying updates.

This design layers on top of `.specs/data-delete-flow` and reuses its key rollout strategy:

- no new wire message types (`load`, `known`, `content`, `done` stay unchanged),
- mixed-version safety by **ignoring** disallowed sessions,
- and “quenching” via a known-state merge response to stop older peers from retrying forever.

## Goals

- Allow an admin to resurrect a deleted coValue while starting from an explicitly new life.
- Make the “new life” easily identifiable in sync + storage by **session ID suffixes**.
- Avoid storing/processing lots of irrelevant history uploaded by mixed-version peers.
- Keep deterministic convergence when there are competing delete/resurrection events.

## Non-goals

- Restoring old data (“undelete with history”).
- Resurrecting Account/Group coValues.
- Adding protocol version negotiation.

## Key design choices

### 1) Lifecycle events are stackable

Delete and resurrection are modeled as **events** that can happen multiple times. Peers may see them in different orders due to offline work. We resolve conflicts deterministically (see “Lifecycle resolution”).

### 2) Session filtering is the core mechanism

To separate “this life” from “old life (should have been deleted)”, we accept updates only from sessions tagged with the **active resurrection ID**.

Session IDs for the active life must be of the form:

- `${BaseSessionID}_r${ResurrectionID}`

This enables cheap suffix checks in hot sync/storage paths.

### 3) Resurrection begins with a trusting marker (txIndex === 0)

A resurrection starts with a **trusting** transaction that carries `meta.resurrectionId`.

Constraints:

- it must be the **first transaction of its session** (`txIndex === 0`),
- and `meta.resurrectionId` must match the `ResurrectionID` encoded in the session ID.

This makes resurrection explicit and verifiable, and lets peers reject malformed resurrection attempts early.

### 4) Admin-at-time validation

Resurrection is valid only if the author was **admin at the time of the transaction** (`tx.madeAt`), just like delete.

Account and Group coValues are not resurrectable.

## Data model

### IDs and session shapes

We introduce:

- `ResurrectionID`: an opaque ID (random, collision-resistant; format TBD but must be session-safe).
- `ResurrectionSessionID`: `${SessionID}_r${ResurrectionID}`

Existing:

- `SessionID`: `${RawAccountID | AgentID}_session_z${string}`
- `DeletedSessionID`: `${SessionID}_deleted`

### Transaction meta

Delete marker (existing, extended):

```ts
type DeleteMeta = {
  deleted: true;
  // New: which life is being deleted.
  // If omitted/undefined, it means the “base life” (no resurrection suffix).
  deletedResurrectionIds?: ResurrectionID;[]
};
```

Resurrection marker:

```ts
type ResurrectionMeta = {
  resurrectionId: ResurrectionID;
};
```

Notes:

- Both markers are **trusting** transactions.
- The resurrection marker must be the first tx in its session.
- The delete marker remains “one tx in a dedicated delete session” as in `.specs/data-delete-flow`.

## Lifecycle resolution

Each coValue has a computed lifecycle state:

- `Active(base)` (no resurrection yet)
- `Active(resurrectionId = R)`
- `Deleted(deletedResurrectionId = undefined | R)`

We define a deterministic rule to pick the current state from a set of observed events.

### Valid event definitions

An event is considered for lifecycle resolution only if:

- The coValue is not Account/Group.
- The marker transaction is syntactically valid:
  - delete: trusting + meta.deleted === true + in a `_deleted` session + only tx in that session
  - resurrection: trusting + `meta.resurrectionId` + `txIndex === 0` + session ID suffix `_r${meta.resurrectionId}`
- The author was admin at `tx.madeAt` (unless `skipVerify` mode, see below).

### Ordering

We order valid events by:

1. `tx.madeAt` ascending
2. tie-breaker: `sessionID` lexicographically ascending
3. tie-breaker: `txIndex` ascending (should be 0 for resurrection markers; delete is always “single tx”)

### Applying events

Start state is `Active(base)`.

Apply events in order:

- **Resurrection marker** with id `R`:
  - sets state to `Active(resurrectionId = R)`
- **Delete marker**:
  - sets state to `Deleted(deletedResurrectionId = deletedResurrectionIdFromMetaOrCurrentState)`
  - where `deletedResurrectionIdFromMetaOrCurrentState` is:
    - `meta.deletedResurrectionId` if present
    - otherwise “the resurrection id that was active when this delete was authored” (best-effort; for deterministic replay we recommend always writing it in meta)

Final lifecycle state is the result after all events.

## Sync + ingestion behavior

The key rule is: **apply content only from allowed sessions**, but still allow lifecycle markers to arrive even when content is currently blocked.

### Session classification helpers

We treat sessions as:

- **Delete sessions**: `sessionID.endsWith("_deleted")`
- **Resurrection-tagged sessions**: `sessionID.includes("_r")` and ends with `_r${someResurrectionId}` (exact parsing helper)
- **Base sessions**: everything else

### Inbound `content` filtering

When receiving a `content` message for a coValue:

1. Always accept header if needed (header is required to validate permissions).
2. Always consider delete sessions for ingestion (as in delete design).
3. Consider resurrection markers:
   - If a session looks like a resurrection-tagged session, we only accept `txIndex === 0` unless the session’s resurrection id is currently active.
   - After ingesting a valid marker, recompute lifecycle resolution (it can flip from deleted → active).
4. After lifecycle state is known:
   - If state is `Deleted(...)`:
     - accept only delete sessions and (potential) resurrection marker tx0.
     - ignore all base sessions and all resurrection-tagged sessions except marker tx0.
   - If state is `Active(base)`:
     - accept base sessions
     - accept delete sessions
     - ignore resurrection-tagged sessions (unless they contain a valid marker tx0; marker ingestion is allowed to discover a newer resurrection event)
   - If state is `Active(resurrectionId = R)`:
     - accept delete sessions
     - accept resurrection-tagged sessions **only** if they end with `_rR`
     - ignore base sessions and any other resurrection-tagged sessions

### Outbound known-state + quenching

When we ignore sessions offered by a peer (because they’re not allowed by the current lifecycle state), older peers may retry indefinitely.

We use the same quenching strategy as delete:

- When we detect we are intentionally ignoring sessions, respond with a `known` message that merges our current knownState with the peer’s knownState sessions (“make them believe we already have it”).

This should be used for:

- `Deleted(...)` state (already implemented for delete)
- `Active(resurrectionId = R)` where base sessions / non-matching resurrection sessions are ignored

## Storage behavior

### Storage must not “re-activate” ignored sessions

Storage should follow the same acceptance rules as sync ingestion:

- store and serve only sessions that would be accepted for the current lifecycle state,
- plus lifecycle marker sessions needed for lifecycle resolution.

This prevents older peers from flooding storage with irrelevant sessions.

### Physical erasure (“space reclamation”) must be life-aware

The existing delete erasure primitive deletes everything except the tombstone delete session(s).

With resurrection, erasure must be more precise:

- If the coValue is currently `Deleted(deletedResurrectionId = undefined)` (“base life deleted”):
  - erase **base sessions only**
  - keep delete sessions (`*_deleted`)
  - keep all resurrection-tagged sessions (`*_r<...>`) (future or past resurrections must not be erased)
- If the coValue is currently `Deleted(deletedResurrectionId = R)` (“resurrection life R deleted”):
  - erase only sessions tagged with `_rR`
  - keep delete sessions
  - keep base sessions and other resurrection-tagged sessions (they belong to other lives)

To make this safe and deterministic across peers, delete markers should include `meta.deletedResurrectionId` whenever the active life is a resurrection life.

## Skip-verify environments

Some storage shards run with `skipVerify: true` and do not verify permissions.

For resurrection:

- In skip-verify mode, shards may store lifecycle markers without validation.
- Higher-trust nodes (clients/servers that verify) must still enforce admin-at-time validation when deciding lifecycle state and when serving/syncing.

## Error handling / edge cases

### Invalid resurrection marker

Reject the marker (do not change lifecycle) if any of the following holds:

- not trusting
- missing `meta.resurrectionId`
- `txIndex !== 0`
- session suffix resurrection id doesn’t match `meta.resurrectionId`
- author not admin at `tx.madeAt`

### Competing offline events

Competing delete/resurrection events may exist (e.g. two admins resurrect while offline).

Resolution is deterministic (“latest valid event wins”), but partially deleted storage can cause temporary inconsistent UX:

- one peer may have already physically erased sessions that another peer still considers relevant,
- the system may surface “deleted”/“active” inconsistently until all peers converge on the same event set.

This is a known risk (see “Risks”).

## Testing strategy

Add tests that mirror the existing delete tests (see `sync.deleted.test.ts`) but with resurrection:

- Admin can resurrect a deleted coValue; post-resurrection only `_rR` sessions are accepted.
- Old base sessions are ignored after resurrection (including unknown-at-delete sessions).
- Quenching: older peers stop retrying ignored sessions when we send merged knownState.
- Competing events: two resurrections, latest valid wins deterministically.
- Storage erasure:
  - base-life deletion preserves resurrection-tagged sessions
  - deletion of resurrection life `R` erases only `_rR` sessions

## Rollout notes

- Avoid wire protocol changes.
- Keep suffix checks cheap and on hot paths (`sync.ts`, storage adapters).
- Ensure older peers do not get stuck: quench whenever we intentionally ignore sessions.

## Risks

- **Offline conflicts**: deterministic, but can surprise users if two admins resurrect/delete offline.
- **Partial physical deletion**: may produce inconsistent statuses until systems re-sync and converge.
- **Mixed-version flooding**: mitigated by suffix filtering + quenching, but still requires careful performance testing on noisy peers.


