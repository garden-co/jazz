# Object Manager Merge + Commit Payload Research — TODO (This Week)

Tried to play through scenarios, but key parts are still unclear.

Main concerns:

- If commits store full snapshots, how do we optimize transmission when peers need only small updates on specific fields?
- How do we support custom merge strategies if full snapshots hide the user intent/action shape?
- When exactly should tip merge happen?

## Research Task 1: Can Current Snapshot Design Be Kept?

Research whether these concerns can be solved while staying on the current full-snapshot commit design, or whether the design should move to delta-style payloads.

### Questions to answer

1. Can we send compact field-level deltas over the wire while still storing snapshots?
2. If yes, what metadata must be added to preserve user-intent needed for merge strategies?
3. If no, what is the minimum change needed in commit representation?
4. What are the storage, sync bandwidth, and read-path tradeoffs for:
   - full snapshots only
   - snapshot + derived delta encoding
   - delta-first commits (+ optional checkpoints)
5. Which approach keeps behavior deterministic across peers?

### Expected output

- A recommendation: **keep current design** or **change design**, with decision criteria and risks.
- A short comparison table (complexity, bandwidth, storage, merge flexibility, determinism).

## Research Task 2: Tip-Merge Timing and Policy

Define when tips should merge, and when they should stay diverged.

### Scope

- Auto-merge conditions
- Explicit/manual merge conditions
- Deferred merge conditions (for missing deps / incomplete context)
- Conflict artifact conditions (cannot auto-resolve)

### Required user stories (EARS style)

Define user stories that describe expected behavior for all key scenarios, including:

- **When** concurrent edits touch disjoint columns, **the system shall** auto-merge tips.
- **When** concurrent edits touch the same column and strategy is deterministic, **the system shall** resolve and merge tips.
- **When** concurrent edits touch the same column and strategy is non-deterministic or missing, **the system shall** keep tips diverged and emit a conflict artifact.
- **When** a merge commit references all current tips, **the system shall** collapse frontier to one tip.
- **When** sync receives out-of-order commits, **the system shall** defer merge evaluation until dependencies are present.
- **While** tips remain diverged, **the system shall** expose all tips in deterministic order to subscribers.

### Expected output

- Merge policy spec draft with invariants and trigger points.
- Scenario matrix mapping input condition -> merge outcome -> subscription/global update behavior.
