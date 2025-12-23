# Requirements: Deleted coValues resurrection

## Introduction

We already support deleting coValues (see `.specs/data-delete-flow`). Deletion blocks sync of historical content and leaves a tombstone.

This spec adds a new lifecycle phase: **resurrection**.

Resurrection lets users bring a deleted coValue back **without restoring old data**. Instead, resurrection starts a new “life” from an explicit **init/reset transaction**. Anything from before that resurrection must stay ignored (even if older peers keep trying to upload it).

Key constraints:

- Delete and resurrection are **stackable events** (multiple deletes/resurrections may exist).
- Delete and resurrection events must be created in **dedicated sessions** so they can be cheaply identified.
- Updates are accepted only from sessions tagged with the **active resurrection ID** (suffix check is enough).
- Resurrection is valid only if the author was **admin at the time of the transaction**.
- Account and Group coValues are **not resurrectable**.

## User stories + acceptance criteria (EARS)

### US1 — Admin-only resurrection API

As a developer, I want an explicit API to resurrect a deleted coValue so users can bring it back intentionally.

- **AC1.1 (When/Then)**: When code calls `coValue.$jazz.raw.core.resurrectCoValue(...)`, then the method checks that the coValue is not an Account or Group coValue.
- **AC1.2 (If/Then)**: If the coValue is an Account or Group coValue, then the method throws an error.
- **AC1.3 (When/Then)**: When code calls `resurrectCoValue(...)`, then the method checks that the current account has admin permissions on the coValue.
- **AC1.4 (If/Then)**: If the current account is not an admin, then the method throws an error.
- **AC1.5 (When/Then)**: When `resurrectCoValue(...)` succeeds, then it creates a resurrection init/reset transaction as described in US3–US4.

### US2 — Dedicated sessions for delete + resurrection events

As the system, I want delete and resurrection to be modeled as dedicated-session events so peers can filter cheaply and deterministically.

- **AC2.1 (When/Then)**: When a delete operation is performed, then the delete marker transaction is created in a dedicated delete session (as defined in `.specs/data-delete-flow`).
- **AC2.2 (When/Then)**: When a resurrection is performed, then the resurrection init/reset transaction is created in a dedicated resurrection session of the form `${BaseSessionID}_r${ResurrectionID}`.
- **AC2.3 (While)**: While syncing / storing content, the implementation can decide whether a session is “resurrection-scoped” using a cheap suffix check (no JSON parsing required for the common case).

### US3 — Resurrection init/reset marker semantics

As the system, I want resurrection to begin with a single trusting init/reset marker so a new life is explicit and can be validated.

- **AC3.1 (When/Then)**: When a resurrection starts, then the first transaction in the resurrection session has `privacy: "trusting"` and includes `meta.resurrectionId` equal to the session’s `ResurrectionID`.
- **AC3.2 (When/Then)**: When ingesting a resurrection session, then the transaction that carries `meta.resurrectionId` must be the first transaction in the session (`txIndex === 0`), otherwise the resurrection marker is invalid.
- **AC3.3 (While)**: While processing transactions, resurrection marker detection must not require parsing meta for non-resurrection sessions.
- **AC3.4 (When/Then)**: When a valid resurrection marker is accepted, then the coValue is considered “resurrected” and eligible to accept non-marker updates only from sessions allowed by US4.

### US4 — Session filtering: accept only sessions for the active resurrection

As the system, I want to accept updates only from sessions tagged with the active resurrection ID so old sessions stay ignored forever (even if discovered later).

- **AC4.1 (When/Then)**: When a coValue is in a resurrected state with active `ResurrectionID = R`, then the system accepts new transactions only from:
  - the current active resurrection session(s) tagged with `_rR`, and
  - any other session types explicitly allowed by the protocol (e.g. future lifecycle events).
- **AC4.2 (When/Then)**: When the system receives content for a session whose ID does not match the active resurrection suffix `_rR`, then it ignores that session’s content (it must not be applied, stored, or forwarded).
- **AC4.3 (While)**: While interacting with mixed-version peers, this filtering must be sufficient to prevent uploading/storing lots of irrelevant history.

### US5 — Admin-at-time validation for resurrection

As the system, I want resurrection to be valid only if the author was admin at the time the resurrection marker was written so offline peers can’t resurrect with stale permissions.

- **AC5.1 (When/Then)**: When ingesting a resurrection marker transaction, then the system validates that the author had admin permissions on the coValue at `tx.madeAt`.
- **AC5.2 (If/Then)**: If admin-at-time validation fails, then the resurrection marker is rejected and must not change the coValue lifecycle state.
- **AC5.3 (While)**: While running in `skipVerify: true` storage environments, the system may store the marker without validation, but higher-trust components must still enforce the validation semantics when serving/syncing.

### US6 — Deterministic lifecycle resolution with competing deletes/resurrections

As the system, I want a deterministic rule for resolving competing events (offline conflicts) so all peers converge.

- **AC6.1 (When/Then)**: When multiple delete/resurrection events exist, then the active lifecycle phase is determined by ordering the **valid** events and taking the latest applicable one (“latest valid event wins”).
- **AC6.2 (If/Then)**: If an event is invalid (e.g. non-admin-at-time resurrection), then it is ignored for lifecycle resolution.
- **AC6.3 (While)**: While lifecycle resolution is deterministic, the system may temporarily surface inconsistent statuses if storage erasure partially removed content; this must be documented as a known risk.

### US7 — Mixed-version safety + quenching

As the system, I want mixed-version peers to stop endlessly trying to upload sessions that are ignored by resurrection filtering.

- **AC7.1 (When/Then)**: When a peer offers content for sessions that are not allowed by US4, then the receiver ignores them.
- **AC7.2 (When/Then)**: When interacting with older peers that keep retrying, then the receiver replies with a “quenching” known-state response that makes the sender believe the receiver already has the sender’s known sessions (even if they are intentionally ignored).
- **AC7.3 (While)**: While rolling out, this must work without introducing new wire message types.

### US8 — Storage safety: deletions must be able to erase only deleted sessions

As the system, I want deletes to be able to erase only sessions that are actually part of the deleted life, without accidentally erasing the new life.

- **AC8.1 (When/Then)**: When creating a delete marker, then it includes enough information about **known resurrections at the time of delete** to allow storage erasure to delete only the sessions belonging to the deleted life.
- **AC8.2 (When/Then)**: When performing physical deletion/erasure for a deleted coValue, then storage deletes only sessions belonging to the deleted life and preserves tombstone(s) and any resurrected-life sessions.
- **AC8.3 (While)**: While older peers may upload sessions not known at delete time, the filtering rules (US4, US7) must prevent these sessions from reappearing as “live” content.

## Out of scope (for this feature)

- Restoring historical content from before a delete (“undelete with old data”).
- Making resurrection available for Account/Group coValues.
- Introducing new sync wire message types or protocol version negotiation.
- Guaranteeing perfect UX under partial erasure + offline conflicts (we only require deterministic convergence rules).


