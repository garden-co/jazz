# Problem Statement: Sync Protocol Reliability

## What's broken

The current sync protocol was not designed with reliability as a first-class property. It works in the happy path but provides no guarantees when things go wrong — and in a distributed system, things always go wrong. The result: a local change can look successful to the user but silently fail to reach other devices.

This is not a collection of bugs to patch. The gaps below are structural — symptoms of a protocol that lacks delivery confirmation, ordering guarantees, and loss detection. The project is a **protocol redesign** with reliability as a foundational goal.

Five identified reliability gaps that compound each other:

1. **Out-of-order payload arrival corrupts branch tips.** The sender topologically sorts commits (parents before children) within a single payload, but every write produces a Spawn (async task), so even consecutive writes generate separate payloads that race. These payloads can arrive at the receiver out of order. The receiver's `receive_commit()` accepts a child commit without checking that its parents exist yet. When a parent hasn't arrived, `branch.tips.remove(parent)` is a no-op, so both the orphaned child and the late-arriving parent end up as separate tips. This corrupts the branch tip set: queries see phantom merge states, subscriptions emit spurious deltas, and downstream sync propagates the wrong frontier. The spec claims "topological sort ensures parent-before-child" (INV-S in `sync_manager.md`), but that invariant only holds within a single payload — the transport layer provides no cross-payload ordering guarantee. **This is inherent to the write path** — every write spawns a task, so any sequence of writes can hit it under normal usage, not just explicitly parallel operations.

2. **Outbox drained before delivery confirmation.** The client clears its outbox as soon as it hands messages to the transport — before the server acknowledges receipt. If the connection drops mid-flight, those messages are gone. The client believes they were sent; the server never saw them.

3. **Lost message poisons incremental sync.** SyncManager tracks `sent_tips` to send only new commits. If one message is lost, the gap is never detected. All subsequent incremental syncs build on a foundation the receiver doesn't have, and the divergence is permanent until a full resync (which only happens on `ClientId` change).

4. **Server response body ignored.** The `POST /sync` endpoint returns a `SyncBatchResponse` with per-message results (success, permission errors, etc.). The TypeScript client fires the POST and discards the response. Server-side write failures are invisible to the client — the user sees no error, no retry, no indication that their data didn't persist.

5. **Asymmetric reconnect recovery.** On reconnect, the client replays all active `QuerySubscription`s, which repairs the receive side (server re-sends scoped data). But the send side has no equivalent mechanism — any outbound messages lost before the disconnect stay lost. The client-to-server direction can silently diverge.

### Theoretical concern (to verify)

- **Data and control messages share a fragile path.** `ObjectUpdated`, `QuerySubscription`, `PersistenceAck`, and `Error` all flow through the same channel. A backlog of large data payloads could delay delivery of time-sensitive control messages (subscriptions, acks, errors), and a malformed payload in one type could potentially disrupt the entire stream. No concrete incident has been observed — this is an architectural risk to validate during design.

### Design constraint: transport-agnostic protocol

Whatever reliability solution we build must work as the communication layer for **client↔worker** (postMessage) in addition to **client↔server** (HTTP/SSE). The protocol is the protocol regardless of transport — reliability guarantees must hold across all channels.

### Evidence

- **Pre-production framework.** Jazz is not yet in production, so these gaps are identified through testing and code review rather than production incidents.

- **Ignored regression test:** `subscription_reflects_final_state_after_rapid_bulk_updates` in `crates/jazz-tools/tests/subscribe_all_integration.rs` (line 459) is `#[ignore]` with explicit reference to these gaps. The test sends 500 rapid updates from alice and verifies bob's last subscription delta matches his snapshot query. It fails because out-of-order payload arrival corrupts the branch tip set (gap 1), causing the subscription stream to diverge from the snapshot.

- **Duplicated state machines:** Both `packages/jazz-tools/src/runtime/client.ts` and `packages/jazz-tools/src/worker/jazz-worker.ts` implement independent reconnect/auth/streaming logic. Bug fixes applied to one are easily missed in the other (tracked in `todo/issues/duplicated-sync-transport-state-machines.md`).

- **Inconsistent reconnect strategies:** The Rust client uses a fixed 5-second retry loop (`client.rs:157-257`) while the TypeScript browser client uses exponential backoff with jitter (`sync-transport.ts:192-202`). Same logical operation, different behavior, maintained separately.

## Who is affected

**All Jazz consumers.** These gaps affect every application built on Jazz:

- **App developers** get no signal when writes fail server-side — they can't surface errors to their users or implement retry logic because the client SDK tells them everything succeeded.
- **End users of Jazz-powered apps** lose data silently. A note edited offline, a form submitted on a flaky connection, a collaborative edit during a brief disconnection — any of these can vanish without warning.
- **Server operators** running multi-tier deployments (edge + global) face compounding issues: a lost message at the edge→global hop is invisible to the originating client and permanently diverges the tiers.

## Concrete examples

### Example 1: Concurrent Spawns corrupt branch tips

Alice's app spawns three related objects in parallel (e.g., a Project, a TaskList, and a first Task — each via a separate `Spawn` call). Each Spawn produces a commit sent in its own payload. The sender topologically sorts within each payload, but the payloads themselves race across the network. If the Task payload (parent: TaskList) arrives before the TaskList payload, the server's `receive_commit()` accepts the Task commit anyway — `tips.remove(TaskList)` is a no-op since TaskList hasn't arrived yet. When TaskList finally arrives, it also becomes a tip. Now the branch has two tips {TaskList, Task} instead of {Task}. Bob's subscription sees this as a merge state, emitting spurious deltas. This is normal app behavior — every write spawns a task, so any sequence of writes can trigger it.

### Example 2: Offline queue loss

Charlie makes 12 edits while on a train with spotty connectivity. The client queues them and starts sending when the connection briefly returns. The connection drops after 8 messages are sent. The client has already cleared all 12 from its outbox. On reconnect, it has nothing left to send — those 4 edits are permanently lost. Charlie's device shows all 12 edits; every other device shows only 8.

### Example 3: Silent server rejection

Diana's backend has row-level permission policies. She submits an edit that the server rejects (`SyncError::PermissionDenied`). The server returns this in the `SyncBatchResponse`, but the client ignores the response body. Diana's local state shows the edit as applied. It never syncs to anyone else. If she refreshes, the edit vanishes — or worse, reappears if her local state is treated as authoritative.

### Example 4: Multi-tier divergence

An edge server receives a write from a client and applies it locally. It forwards the write to the global server, but the message is lost in transit. The edge server has already updated its `sent_tips`. On the next batch, it sends only newer commits — the lost commit is never retried. The edge and global tiers are permanently diverged for that object until a manual intervention or full resync.
