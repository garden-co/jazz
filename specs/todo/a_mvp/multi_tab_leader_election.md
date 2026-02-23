# Multi-Tab Leader Election — MVP Design

## Problem

Browser OPFS durability uses `FileSystemSyncAccessHandle`, which is exclusive per file. Today only one tab can own the durable worker state at a time.

We need multi-tab coordination where:

- One tab is the logical leader and serves other tabs
- Followers stay usable while leader is alive
- Leadership can fail over when the leader disappears or stalls

## Key Constraints

- `createSyncAccessHandle()` is async, so lock acquisition is an async boundary.
- `createSyncAccessHandle()` is available in Dedicated Workers (not SharedWorker/ServiceWorker/Window).
- There is no cross-tab "force unlock" API for a stale lock holder.
- Background tabs can be throttled, frozen, or discarded; heartbeat delays can be very large (or effectively unbounded).
- Dedicated workers are lifecycle-bound to their owning tab; if the tab is frozen, worker progress can stall.
- Worker contexts do not get a direct backgrounding signal; tab lifecycle hints must come from the page.

## Goals

- Preserve a single logical leader per app/db namespace at a time.
- Keep the storage/query fast path synchronous once leader owns the lock.
- Avoid split-brain via strict fencing.
- Keep followers operational during leader changes.
- Accept fire-and-forget durability semantics for MVP (possible loss around crashes/failover).

## Non-Goals (MVP)

- Perfect zero-loss local durability during abrupt termination.
- Guaranteed immediate lock recovery after every freeze/crash.
- Cross-tab lock preemption.

## Design Summary

- Use lease-based leader election over tab-to-tab messaging (`BroadcastChannel` in MVP).
- Elect a logical leader term first; leader is active only after successful OPFS lock acquisition.
- Leader keeps OPFS handle open for its tenure (no per-op acquire/release).
- Followers route sync traffic through leader.
- Visibility/focus is a scheduling hint, not a hard election trigger.

## Current Main-Thread/Worker Sync Shape

This design builds on existing runtime behavior:

- Main thread runtime emits sync outbox entries as `{ destination, payload }`.
- Main thread bridge forwards only `destination=Server` payloads to worker.
- Worker runtime treats main thread as a `Client` (`addClient`, role `peer`) and ingests payloads via `onSyncMessageReceivedFromClient`.
- Worker runtime forwards `destination=Client` payloads back to main thread and `destination=Server` payloads to HTTP sync.
- Main thread applies incoming worker payloads with `onSyncMessageReceived`.

Server attach semantics already exist and are important:

- `removeServer()` detaches current upstream server edge.
- `addServer()` re-attaches and forces replay/full-sync behavior.
- Re-attach is currently used for stream reconnect in both JS client and worker paths.

## Leader Change Integration with Upstream Server Semantics

Leader change must be treated as upstream server change for every tab runtime.

Rule:

- On effective leader switch, each tab must call `runtime.removeServer()` then `runtime.addServer()` for its main-thread runtime once the new leader transport is ready.

Why this is required:

- `addServer()` in WASM runtime performs re-attach semantics (remove + add of upstream edge) and flushes outbox.
- QueryManager `add_server` replays active query subscriptions to the newly attached upstream.
- This is the exact behavior needed when changing leader tabs.

## Per-Tab Upstream State Machine

Each tab tracks:

- `currentTerm`
- `currentLeaderTabId`
- `upstreamState`: `detached | connecting | attached`

On `leader-active(term, leaderTabId)`:

- Ignore if `term < currentTerm`.
- If `term > currentTerm`, fence old leader traffic and update local term.
- If leader identity changed:
  - Enter `detached`.
  - Call `runtime.removeServer()`.
  - Tear down old leader transport route.
  - Connect route to new leader.
  - On route-ready: call `runtime.addServer()` and enter `attached`.

This applies to all tabs, including the old leader tab after it steps down.

## Transport Routing Details

Follower tab routing:

- Outgoing local runtime payloads with `destination=Server` are sent to current leader route (not HTTP directly).
- Incoming payloads from leader route are applied via `runtime.onSyncMessageReceived(payload)` only when `term == currentTerm`.

Leader tab routing:

- Leader worker keeps one runtime client for its own main thread (existing behavior).
- For each follower peer, leader worker maintains a distinct runtime client mapping.
- Incoming follower payloads are fed to worker runtime using `onSyncMessageReceivedFromClient(mappedClientId, payload)`.
- Outgoing worker payloads with `destination=Client(mappedClientId)` are routed back to that specific follower peer.

## Protocol Extensions (MVP)

Existing worker protocol is single-client (`sync` between one main thread and one worker). Multi-tab leader routing needs explicit peer channels.

Recommended minimal extension:

- Main -> Worker:
  - `peer-open { peerId }`
  - `peer-sync { peerId, term, payload[] }`
  - `peer-close { peerId }`
- Worker -> Main:
  - `peer-sync { peerId, term, payload[] }`

Notes:

- `sync` remains reserved for local tab main-thread <-> worker traffic.
- `peer-sync` carries term for fencing.
- Leader main thread can broker BroadcastChannel traffic and forward to worker via these peer messages.

## Query Replay and Settled Semantics Across Leader Swaps

- Upstream swap (`removeServer` + `addServer`) intentionally replays query subscriptions.
- Upstream swap also replays object state to the new upstream via full-sync semantics on `addServer`.
- During swap, query updates may pause until replay settles on the new leader.
- Queries waiting on settled tiers (`worker`/`edge`/`core`) may await fresh `QuerySettled` from the new upstream path.
- Any late `QuerySettled` from old term must be dropped by term fence rules.

No explicit bridge-level queue is required for correctness during swap:

- Writes/subscriptions performed while detached are covered by runtime/state replay after `addServer()`.
- A short-lived queue can still be used as an optional optimization to reduce visible latency, but should not be required for convergence.

## Connection Cleanup Caveat

Current WASM bindings expose `addClient` and `setClientRole`, but do not expose `removeClient`.

MVP behavior:

- Peer client mappings in leader worker may be retained until worker restart when followers disconnect abruptly.

Future improvement:

- Expose/remove peer clients explicitly on `peer-close` to avoid stale client state accumulation.

## Election and Liveness

Each tab has a stable `tabId` and tracks:

- `currentTerm` (monotonic)
- `currentLeaderId`
- `leaderLeaseExpiresAt`

Election behavior:

- New election starts when lease expires or leader explicitly steps down.
- Candidate increments term and announces candidacy.
- Tie-break by `(term, tabId)` if needed.
- Winner becomes provisional leader, then attempts OPFS lock acquire.
- Only after lock acquire succeeds does it announce `leader-active`.

Hard re-election triggers:

- Explicit leader shutdown/step-down
- Lease timeout (missed heartbeats)
- Leader cannot initialize/retain durable storage

Soft handoff behavior:

- On leader tab becoming hidden, leader may offer handoff after a grace period.
- Visible followers may take over.
- Use hysteresis (minimum leader tenure + takeover cooldown) to prevent ping-pong.

## Fencing (Term/Epoch Safety)

All leader-originated messages include `term` and `leaderId`.

Rules:

- `incoming.term > currentTerm`: adopt newer term, step down immediately.
- `incoming.term < currentTerm`: reject as stale.
- `incoming.term == currentTerm`: accept only from `currentLeaderId`.

Follower requests include `expectedTerm`; leader responses echo `term`.
Followers drop responses whose term does not match local `currentTerm`.

This prevents zombie leaders from being accepted after stall/resume.

## OPFS Lock Ownership Model

Leader lock policy:

- Acquire once during leader activation.
- Keep handle open for leader tenure.
- Release on step-down/shutdown and best-effort on lifecycle transitions.

Important: on-demand lock per read/write is not viable for current architecture because acquiring a sync handle is async and would force async boundaries into the synchronous storage/query path.

## Lifecycle Integration

Page listens to:

- `visibilitychange`
- `pagehide`
- `freeze` / `resume` where available

Page forwards lifecycle hints to its dedicated worker via `postMessage`.
Worker performs best-effort actions:

- Flush WAL (where possible)
- Close runtime/handle on step-down or graceful handoff

Correctness does not depend on receiving these hints; lease timeout + fencing remains the safety mechanism.

## Zombie Lock Handling

A zombie lock means a stale tab/worker still holds the primary OPFS file lock after losing logical leadership.

MVP behavior:

- New leader that cannot acquire the primary file enters degraded mode.
- Continue serving live state via memory + server sync.
- Keep retrying primary lock acquisition with backoff.

Optional implementation detail for degraded durability:

- New leader may open a fallback OPFS file namespace (for example, suffix by term/tabId) for durable writes while primary is locked.
- Tradeoff: this preserves new writes but existing local-only primary state is not available until resync/recovery.
- This is acceptable for MVP under fire-and-forget semantics if server sync can rebuild required state.

## Recovery and Reconciliation

When primary lock becomes available:

- Prefer switching back to primary namespace.
- Rebuild state from server sync as source of truth for MVP.
- Followers converge on the active leader term and storage namespace announcement.

## Future Improvements

- Add robust cleanup/GC for fallback OPFS files left behind after zombie-lock incidents or crashes.
- Add explicit metadata/index of fallback namespaces to make cleanup deterministic.
- Explore stronger convergence between fallback durable writes and primary namespace (merge/import tooling).
- Expand browser E2E coverage for freeze/throttle/discard scenarios.
