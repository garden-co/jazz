# Design: Signature Mismatch Recovery with Session Rebase/Replace

## Overview

When the sync server detects a signature mismatch for a client-submitted transaction in a session, the server must return an explicit error to that client including authoritative recovery content for the conflicting session.

Client recovery behavior is role-dependent:

1. If the client owns the conflicting session: run a rebase flow in a temporary node, then replace the local verified content with the rebased verified state.
2. If the client does not own the conflicting session: no-op for now (do not rewrite local storage in this iteration).

Only the owner flow rewrites local storage for the conflicting session in this iteration.

## Architecture / Components

### 1. Protocol Extension: Signature Mismatch Error

Add a new sync error message type:

```ts
type SignatureMismatchErrorMessage = {
  action: "error";
  errorType: "SignatureMismatch";
  id: RawCoID;
  sessionID: SessionID;
  // Authoritative content for the conflicting session
  content: SessionNewContent[];
  reason: string;
};
```

### 2. Server Flow

On signature verification failure in `handleNewContent` path:

1. Stop processing the invalid incoming transaction batch.
2. Build authoritative session content from verified in-memory state or storage.
3. Send `SignatureMismatch` error message back to originating peer with `content: SessionNewContent[]`.
4. De-duplicate by `(peerId, coValueId, sessionID)` so the same mismatch is sent only once per peer lifecycle.

Core behavior:

```ts
if (error.type === "InvalidSignature") {
  const authoritative = coValue.verified.getFullSessionContent(sessionID);
  if (!peer.shouldSendSignatureMismatch(coValue.id, sessionID)) return;
  peer.push({
    action: "error",
    errorType: "SignatureMismatch",
    id: coValue.id,
    sessionID,
    content: authoritative,
    reason: error.message,
  });
  return;
}
```

### 3. Client Recovery Coordinator (Current Implementation)

Add `SessionConflictRecoveryCoordinator` in `LocalNode`.

Responsibilities:

- Validate and normalize authoritative `content: SessionNewContent[]`.
- Determine ownership of `sessionID`.
- Run owner-specific rebase flow in a temporary node or non-owner no-op flow.
- Rewrite local storage session data for `(coValueId, sessionID)` only in owner flow.
- Swap local in-memory verified content with the rebased verified content.
- Centralize recovery error handling/logging at the top-level entrypoint.

Current entrypoint behavior:

```ts
recoverSignatureMismatch(msg) {
  if (!isOwner(msg.sessionID)) return; // no-op for non-owner for now
  if (!local.storage) return; // recovery requires storage

  const normalized = normalizeAuthoritativeSessionContent(msg.content);
  if (!normalized.ok) return;

  runOwnerRecovery(msg, normalized.value).catch((error) => {
    logger.error("Failed to run owner SignatureMismatch recovery", {
      id: msg.id,
      sessionID: msg.sessionID,
      error: error.message,
      errorCode:
        error instanceof SessionConflictRecoveryError
          ? error.code
          : "UNEXPECTED",
    });
  });
}
```

Normalization step:

1. Sort `content` by `after`.
2. Validate continuity (`after` aligns with reconstructed length).
3. Flatten `newTransactions` into a full ordered session transaction list.
4. Take `lastSignature` from the final normalized entry.
5. Emit canonical replacement `content` for storage rewrite.

### 4. Ownership Decision

Ownership check:

```ts
const sessionOwner = accountOrAgentIDfromSessionID(sessionID);
const isOwner =
  sessionOwner === node.getCurrentAccountOrAgentID() ||
  sessionOwner === node.getCurrentAgent().id;
```

### 5. Non-owner Recovery (No-op for now)

If not owner:

1. Record/trace that a signature mismatch was received for a non-owned session.
2. Do not replace local session history in this iteration.
3. Return without rebase.

```ts
if (!isOwner) {
  logger.info("Skipping non-owner SignatureMismatch recovery for now", {
    id,
    sessionID,
  });
  return;
}
```

### 6. Owner Recovery (Current Temp-Node Rebase Flow)

If owner:

1. Build `OwnerRecoveryContext` with:
   `msg`, `coValue`, `tempNode`, `contentWithoutSession`, `normalized`.
2. `buildTempRebasedNode(...)`:
   - Load coValue in a temporary node as current agent.
   - `internalDeleteCoValue(id)` to start from an empty isolated state.
   - Compute `contentWithoutSession` from `newContentSince(...)` by excluding the conflicting session.
   - Apply both non-conflicting local content and normalized authoritative session content via `applyRecoveryInputToTempNode(...)`.
3. `resolveAndRebuildIfDependenciesMissing(...)`:
   - If `tempNode.getCoValue(id).missingDependencies` is non-empty:
   - Wipe temp coValue again.
   - Connect main node to temp as server and `loadCoValueCore(dep)` for each missing dependency.
   - Re-apply recovery input.
   - If dependencies are still missing, fail with typed recovery error.
4. Validate rebased availability:
   - Read `rebasedCoValue = tempNode.getCoValue(id)`.
   - If unavailable, log skip and return.
5. `replayLocalTailTransactions(...)`:
   - Wait until authoritative session content is fully applied in temp (`knownState().sessions[sessionID] === normalized.transactions.length`).
   - Compute common prefix between original local session transactions and authoritative normalized transactions.
   - Build replay tail from local transactions after common prefix.
   - For each tail tx, build `VerifiedTransaction`, decrypt private tx fields if needed, validate parsed changes/meta, and replay via `makeTransaction(...)`.
6. `persistAndSwapRecoveredState(...)`:
   - Persist `rebasedCoValue.verified.getFullSessionContent(sessionID)` using `replaceSessionHistory`.
   - Replace in-memory verified state on original coValue via `coValue.replaceVerifiedContent(rebasedCoValue.verified)`.

Pseudo-flow:

```ts
const context = await buildTempRebasedNode(msg, coValue, normalized);
await resolveAndRebuildIfDependenciesMissing(context);

const rebasedCoValue = context.tempNode.getCoValue(msg.id);
if (!rebasedCoValue.isAvailable()) return;

const replayedTailCount = await replayLocalTailTransactions(context, rebasedCoValue);
persistAndSwapRecoveredState(context, rebasedCoValue);
```

### 6.1 Structured Recovery Errors

Helper methods throw `SessionConflictRecoveryError` with explicit codes:

- `MISSING_DEPENDENCIES_UNAVAILABLE`
- `MISSING_DEPENDENCIES_AFTER_REBUILD`
- `REPLAY_MISSING_PARSED_CHANGES`
- `REPLAY_INVALID_METADATA`

Only `recoverSignatureMismatch(...)` catches and logs these errors.

### 7. Local Storage Rewrite Contract

Add a storage API operation that performs atomic session replacement:

```ts
type ReplaceSessionHistoryInput = {
  action: "replaceSessionHistory";
  coValueId: RawCoID;
  sessionID: SessionID;
  content: SessionNewContent[];
};

interface StorageAPI {
  store(
    data: NewContentMessage | ReplaceSessionHistoryInput,
    correction: CorrectionCallback,
  ): void;
}
```

Routing behavior:

- `store()` dispatches by `action`.
- `action: "content"` goes through the normal `storeSingle` path.
- `action: "replaceSessionHistory"` goes through `storeSingleSessionReplacement`.
- Replacement internally reuses `putNewTxs` per authoritative chunk with forced intermediate signatures.

Required guarantees:

- Atomic replacement (no intermediate mixed history).
- Idempotent for identical payload.
- Safe with concurrent reads (readers see old or new, never partial).

### 8. Recovery State Machine (Conceptual Mapping to Current Code)

Per `(coValueId, sessionID)`:

- `Idle`
- `OwnerGuardAndNormalize`
- `BuildingTempRebaseNode` (`buildTempRebasedNode`)
- `HydratingDependencies` (`resolveAndRebuildIfDependenciesMissing`)
- `Rebasing` (`replayLocalTailTransactions`)
- `RewritingStorageAndSwap` (`persistAndSwapRecoveredState`)
- `Completed`
- `Failed` (typed `SessionConflictRecoveryError` or unexpected error)

For non-owner sessions in this iteration, recovery transitions directly to `Completed` as a no-op.

## Data Models

```ts
type SessionConflictRecoveryState = {
  coValueId: RawCoID;
  sessionID: SessionID;
  owner: boolean;
  phase:
    | "idle"
    | "owner-guard-and-normalize"
    | "building-temp-rebase-node"
    | "hydrating-dependencies"
    | "rebasing"
    | "rewriting-storage-and-swap"
    | "completed"
    | "failed";
  reason: "signature-mismatch";
  rebasedSessionID?: SessionID;
  error?: string;
};
```

```ts
type NormalizedAuthoritativeSession = {
  content: SessionNewContent[];
  transactions: Transaction[];
  lastSignature: Signature;
};
```

## Testing Strategy

We prioritize integration tests with realistic sync/storage interactions.

### Integration Tests (Primary)

1. Server sends authoritative session on signature mismatch
- Given client submits invalid signature for session `S`.
- Then server sends `error: SignatureMismatch` containing `content: SessionNewContent[]` for `S`.

2. Non-owner is no-op
- Given client is not owner of `S` and receives mismatch error.
- Then local storage for session `S` is unchanged in this iteration.
- And no rebase session is created.

3. Owner replays local tail after authoritative replacement
- Given owner receives mismatch error with authoritative `content` for `S`.
- Then a temporary node is built and rebased using authoritative `S` + local tail replay.
- And `replaceSessionHistory` is called with rebased session content.
- And local in-memory verified state is replaced from the temporary rebased state.

4. Convergence after recovery
- Given mixed peers and streaming in progress.
- Then all peers converge with no repeated signature mismatch loop.

5. Storage atomicity
- Given crash/restart during recovery.
- Then storage never exposes partially replaced session history.

6. Load missing dependencies
- Given rebased temp coValue has missing dependencies.
- Then dependencies are loaded from the main node and recovery input is re-applied from empty temp state.

7. Private tail replay failure is surfaced as structured recovery error
- Given a private tail transaction cannot be decrypted/parsed during replay.
- Then replay aborts with `REPLAY_MISSING_PARSED_CHANGES` (or `REPLAY_INVALID_METADATA`) and no `replaceSessionHistory` write occurs.

### Example Test Snippets

```ts
test("non-owner ignores SignatureMismatch recovery in this iteration", async () => {
  // Arrange mismatch for session S not owned by client
  // Act: process SignatureMismatch error message
  // Assert: storage.getSession(id, S) is unchanged
  // Assert: no new local session created
});
```

```ts
test("owner replays local tail after authoritative content replacement", async () => {
  // Arrange local tail diverges from server session
  // Act: process SignatureMismatch error
  // Assert: session S replaced exactly with normalized authoritative content
  // Assert: local tail is re-appended and re-signed
  // Assert: resulting materialized content is deterministic
});
```

## Non-goals (This Iteration)

- Introducing new CRDT semantics.
- Rewriting non-conflicting sessions.
- Non-owner session replacement on signature mismatch (deferred to a follow-up iteration).
- Automatic migration of historical conflicts that are not triggered by current mismatch errors.
