# Leader-Tab Runtime with Port-Broker SharedWorker — TODO (Launch)

Run the durable browser runtime inside a **dedicated `Worker` owned by the leader tab**, and use a thin `SharedWorker` purely as a `MessagePort` broker between tabs and the current leader. Leadership is decided by `navigator.locks`. The persistent OPFS handles and the upstream `/apps/<appId>/ws` socket live in a normal dedicated `Worker`, never in the `SharedWorker`.

An alternative design hosted the durable runtime directly under the `SharedWorker` by having it spawn a dedicated `Worker` for OPFS sync access handles. That nested-worker capability is not portable across our browser matrix (notably Safari), so the runtime cannot live under the SW even indirectly. The leader-tab pattern works around it without giving up the singleton property.

```text
tab A main runtime ─┐                                    tab B (leader)
tab B main runtime ─┼─ port ─> SharedWorker broker ─> dedicated Worker ─> OPFS + /apps/<appId>/ws
tab C main runtime ─┘            (no compute, no            (runtime host,
                                  storage, no sockets)        OPFS + upstream)
```

## Why This Exists

The previous spec relied on `SharedWorker → new Worker(...)` so OPFS `FileSystemSyncAccessHandle` (dedicated-worker-only) could still be reached under a singleton supervisor. That nested-worker call is not reliably available cross-browser. Without it, the runtime must live in a regular `Worker`, which means it must live inside a tab.

We still want the properties the SW design promised:

- one OPFS writer and one upstream socket per `(appId, dbName)`
- no per-tab election state machine in TS
- no per-tab follower routing built on a hand-rolled term/heartbeat protocol
- a single, durable authority from each tab's point of view
- a typed reload/eviction path on protocol or principal mismatch

Roy Hashimoto's wa-sqlite community pattern delivers these without nested workers: the leader tab owns the `Worker`; the SW only hands out `MessagePort`s and announces leader migrations. The OPFS-access-handle pool already works in normal dedicated workers, so this is a topology change, not a storage-engine change.

## Goals

- Run exactly one durable runtime per `(appId, dbName)`, hosted in a dedicated `Worker` inside whichever tab currently holds the `navigator.locks` lease named `jazz:leader:${appId}:${dbName}:v${runtimeProtocolMajor}`.
- Use a `SharedWorker` solely as a port broker and leader directory. The SW must not import the WASM runtime, must not open OPFS, and must not hold any upstream socket.
- Tabs talk to the leader's `Worker` over a `MessagePort` handed out by the broker. Follower tabs do **not** instantiate their own persistent `Worker`.
- Replace `TabLeaderElection`'s term/heartbeat machinery with `navigator.locks` lease ownership. Use `BroadcastChannel` only for "the leader port has moved" notifications, not as the correctness primitive.
- Retarget `WorkerBridge` from `Worker` to a generic `PostMessageEndpoint` so it accepts `Worker`, `MessagePort`, or a `SharedWorker.port`.
- Define a typed "leader migrated" rejection plus a client-side retry contract so an in-flight RPC can survive a leader handoff at most once before surfacing to the caller.
- Keep the main-thread `WasmRuntime` cache in every tab for synchronous local queries.
- Provide a typed fallback for environments without `SharedWorker` and/or `navigator.locks`: memory mode only, no persistence, no cross-tab coherence.

## Non-goals

- No nested workers. The SW does not spawn anything.
- No `SharedArrayBuffer` / cross-origin isolation requirement.
- No React Native, no Node/napi changes. `crates/jazz-rn` and the napi host are single-runtime-per-process and have no multi-tab problem.
- No new election algorithm. `navigator.locks` is the election; we do not invent a layer above it.
- No same-origin multi-principal multiplexing. Mismatched principals on the same `(appId, dbName)` are rejected with `reload-required`, same as the previous spec.
- No `Service Worker` fallback for the broker. SW broker is the only supported brokering path; if it is missing we go to memory mode, not to a hand-rolled BroadcastChannel mesh.

## Core Decisions

### 1. Leadership = a `navigator.locks` lease

A tab becomes leader by acquiring `jazz:leader:${appId}:${dbName}:v${runtimeProtocolMajor}` with `mode: "exclusive"`. While the lock is held, the tab's dedicated `Worker` is the durable runtime.

- The lock-holder callback returns a promise that resolves on **intentional** demotion (e.g. `visibilitychange → hidden` + idle, explicit handoff). It does not resolve on crash; the browser releases the lock automatically.
- All other tabs sit in the lock's queue with `steal: false`. When the lock is released, the browser grants it to the next waiter. No term math, no heartbeat ping, no tiebreak.
- The new leader announces itself on a `jazz:leader-events:${appId}:${dbName}` `BroadcastChannel`. Tabs use this only as a hint to re-request a fresh port from the broker; the broker is still the source of truth.

This replaces the entire `tab-leader-election.ts` + `leader-lock.ts` state machine. The `LeaderLockStrategy` seam stays only as a test seam over `navigator.locks` so we can drive election deterministically in unit tests.

### 2. The `SharedWorker` is a port broker, not a runtime

The SW exposes one operation: "give me a duplex `MessagePort` to the current leader's runtime worker."

Concrete responsibilities:

- accept `connect` events from tabs and store the tab's incoming `MessagePort`
- track which connected port belongs to the current leader (set by the leader tab calling `broker.register(leaderRuntimePort)` after it acquires the lock)
- on each tab connection or on leader change, mint a fresh `MessageChannel`, transfer one end to the tab, transfer the other to the leader
- on leader change, signal each tab's existing port with a `{ type: "leader-migrated" }` message and close it; tabs request a fresh one
- forward nothing else. No DB messages flow through the SW. The SW must not deserialize sync payloads.

Because nothing computational happens in the SW, its bundle stays tiny and its lifecycle (terminate on idle, restart on next connect) is benign.

### 3. The leader tab owns the runtime in a normal dedicated `Worker`

When a tab wins the lock it:

1. constructs a dedicated `Worker(jazz-worker.ts)`
2. runs the existing `WasmRuntime.openPersistent(...)` inside that worker, which holds OPFS handles and the upstream socket
3. publishes its end of leader-side `MessageChannel`s to the broker so the broker can wire newly-arriving tabs to it
4. listens for ports the broker hands it and treats each as a tab session, same shape as today's `MainToWorker` peer channel

When a tab loses the lock (intentional demotion or tab close):

1. it stops accepting new tab ports
2. it drains or rejects in-flight requests with `{ type: "leader-migrated", reason }`
3. it closes the upstream socket and the OPFS handles in deterministic order
4. it terminates the worker
5. the broker promotes whichever tab the OS gave the lock to next

Follower tabs never construct a persistent `Worker`. They reach the runtime only via the broker-supplied port.

### 4. The bridge transport needs the same endpoint abstraction the SW spec proposed

Already needed: `WorkerBridge` accepts `Worker | MessagePort | SharedWorker.port | PostMessageEndpoint`. The Rust side (`WasmWorkerBridge::attach`) accepts an endpoint wrapper instead of `web_sys::Worker`. The worker host stays `DedicatedWorkerGlobalScope` because the runtime side still runs in a dedicated worker — just one inside a tab.

Wire format stays postcard. Each routed message must carry a session id so the leader's runtime can demux multiple tabs' sync streams and durable ack ids.

### 5. In-flight RPC survives a leader handoff at most once

Every call from a follower tab to the leader runtime is tagged with a `request_id`. The protocol distinguishes:

- `Ack` — value resolved or peer-side error
- `LeaderMigrated` — the connection broke mid-call; safe to retry against a new port
- `Conflict` — durable batch was admitted by the previous leader but ack wasn't delivered; client must reconcile via the durable batch id, not blind-retry

The bridge client transparently retries once on `LeaderMigrated` against a freshly-issued port from the broker. A second `LeaderMigrated` in the same call surfaces to the caller — we do not implement infinite retry. Streaming subscriptions terminate with `LeaderMigrated` and re-subscribe with the cursor they last acked.

This is the single most load-bearing piece of new protocol and must be designed before the broker is wired into `Db`.

### 6. Storage reset is a leader-mediated RPC

1. requester sends `deleteClientStorage()` to its broker port
2. leader serializes resets (rejects concurrent ones with `reset-busy`)
3. leader broadcasts `storage-reset-starting` over `BroadcastChannel`; tabs pause their main-thread caches
4. leader closes upstream socket, drops the runtime, releases OPFS handles
5. leader deletes durable storage
6. leader reopens a fresh runtime
7. leader broadcasts `storage-reset-finished`; tabs rehydrate their caches and reconnect their ports

The existing `storage-reset-coordinator.ts` collapses into the leader-side RPC handler. No "term" or "leader id at intent time" fields survive.

### 7. Per-tab main-thread runtime stays

Same justification as the SW spec: synchronous `db.all(...)` is part of the public contract. The main-thread `WasmRuntime` is a cache; the leader-tab `Worker` is the authority.

### 8. Auth compatibility model is unchanged from the SW spec

One principal at a time per `(appId, dbName)` runtime. Token refresh and anonymous→authenticated upgrades are accepted by the leader. Conflicting principals get `auth-mismatch` / `reload-required` synchronously on port handshake.

If we ever need true same-origin multi-principal, storage identity must be partitioned by principal and the `navigator.locks` name must include the partition — explicitly out of scope for launch.

### 9. Version skew handling

`navigator.locks` lock name includes `runtimeProtocolMajor`. Tabs on an incompatible major never compete for the same lock and therefore never share a runtime. The broker port handshake also exchanges minor versions; a minor mismatch the leader can serve is allowed, otherwise the broker closes the port with `reload-required`.

The SW script URL must be stable per release channel (same constraint as the previous spec). The runtime-version BroadcastChannel becomes a pure reload-UX hint.

### 10. Capability matrix and fallback

We require, in this order:

1. `navigator.locks` — required for leader election
2. `SharedWorker` — required for port brokering
3. OPFS sync access handles in dedicated workers — required for persistence

If (1) or (2) is missing we go straight to memory mode. (3) missing also means memory mode. There is no hand-rolled mesh-of-BroadcastChannel fallback; the maintained code paths are exactly:

- leader-tab persistent mode
- memory mode

### 11. Visibility/lifecycle hints

`visibilitychange` / `pagehide` / `freeze` / `resume` still flow per-tab over the broker port. The leader aggregates them. A leader whose own tab goes hidden + idle for long enough should voluntarily release its lock so a more active tab can take over before durable work stalls. This is policy and is tunable; the protocol just exposes the hints.

## What Goes Away

| File / Surface                                                                                                                                                                                             | Disposition                                                   |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `packages/jazz-tools/src/runtime/tab-leader-election.ts`                                                                                                                                                   | Deleted. Replaced by a thin `navigator.locks` wrapper.        |
| `packages/jazz-tools/src/runtime/tab-leader-election.test.ts`                                                                                                                                              | Deleted. New tests cover the lock-based path.                 |
| `packages/jazz-tools/src/runtime/leader-lock.ts`                                                                                                                                                           | Deleted. Test seam moves into the `LocksBackend` abstraction. |
| `packages/jazz-tools/src/runtime/tab-sync-protocol.ts`                                                                                                                                                     | Deleted. Tab↔tab sync now flows through broker-issued ports.  |
| `packages/jazz-tools/src/runtime/storage-reset-coordinator.ts`                                                                                                                                             | Deleted. Folded into leader-side reset RPC.                   |
| `db.ts` follower-routing branches built on the old protocol (`applyBridgeRoutingForCurrentLeader`, `sendFollowerClose`, `resolveWorkerDbNameForSnapshot`, `onLeaderElectionChange`, `adoptLeaderSnapshot`) | Deleted. Replaced by broker-port routing.                     |
| `__fallback__{tabId}` dbName path                                                                                                                                                                          | Deleted. There is no follower in-memory db; followers proxy.  |
| `jazz-tab-sync:*` BroadcastChannel handling                                                                                                                                                                | Deleted.                                                      |

## What Stays / Changes Shape

| File                                                         | Role                                                                                                                                                                                   |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `packages/jazz-tools/src/worker/jazz-shared-worker.ts`       | Port broker. Tiny. No runtime imports.                                                                                                                                                 |
| `packages/jazz-tools/src/worker/shared-worker-supervisor.ts` | Per-tab supervisor: acquires the lock, spawns/terminates the dedicated `Worker` on promotion/demotion, registers the leader-side port with the broker, listens for incoming tab ports. |
| `packages/jazz-tools/src/runtime/shared-worker-control.ts`   | Client-side: connect to broker, request a port, install `leader-migrated` retry.                                                                                                       |
| `packages/jazz-tools/src/worker/jazz-worker.ts`              | Dedicated runtime host, runs **inside the leader tab**. Otherwise unchanged.                                                                                                           |
| `packages/jazz-tools/src/runtime/worker-bridge.ts`           | Retargets from `Worker` to `PostMessageEndpoint`; persistent mode talks to a broker-issued `MessagePort`.                                                                              |
| `crates/jazz-wasm/src/worker_bridge.rs`                      | Accepts a MessagePort-capable endpoint.                                                                                                                                                |
| `crates/jazz-wasm/src/worker_host.rs`                        | Stays `DedicatedWorkerGlobalScope`. Adds per-session demux + `request_id` plumbing.                                                                                                    |
| `crates/jazz-wasm/src/worker_protocol.rs`                    | Adds `request_id`, `session_id`, `LeaderMigrated`, per-peer ack ids.                                                                                                                   |
| `crates/jazz-wasm/src/runtime.rs`                            | Unchanged in shape; still one persistent runtime instance per worker.                                                                                                                  |
| `packages/jazz-tools/src/runtime/db.ts`                      | Significantly simpler. Drops election and the old follower-routing code. New paths: connect-to-broker, retry-on-leader-migrated, leader-side incoming-port handling.                   |

## Test Strategy

- **Lock semantics tests**: pure-Rust/TS unit tests over a `LocksBackend` fake. Cover: single tab promotion, queued waiter promotion on close, queued waiter promotion on voluntary release, no double-leader window across a handoff.
- **Broker tests**: SW broker correctly hands ports to tabs on connect, replaces stale ports on leader migration, and stays alive when the runtime worker is gone (so it can publish the next leader's port).
- **Multi-tab integration tests**: two tabs against one SW. Confirm writes from tab A become visible in tab B; closing tab A promotes tab B; in-flight RPC on tab B during the handoff is retried exactly once and succeeds.
- **Crash test**: hard-kill the leader tab (Web Locks releases). Surviving tab takes over; OPFS handles open cleanly in the new worker.
- **OPFS handoff**: instrumentation confirms previous leader's OPFS handles are released before the new leader's worker opens them.
- **Single-runtime assertion**: across two tabs, exactly one upstream `/apps/<appId>/ws` is observed at a time.
- **Durable ack tests**: `waitForLocalSyncFlush(...)`, durable insert/update/delete, rejected-batch replay, local batch hydration, all driven over the broker port from a follower tab.
- **Reset tests**: tab A requests reset; tab B receives `storage-reset-starting`; OPFS handles close before deletion; a fresh runtime is in place before `storage-reset-finished` fires.
- **Auth mismatch test**: incompatible principal on a second tab gets a typed rejection on port handshake.
- **Capability fallback test**: `SharedWorker === undefined` or `navigator.locks === undefined` → memory mode is selected with no error.
- **Version skew test**: tab on incompatible `runtimeProtocolMajor` does not contend for the lock and gets a typed `reload-required` from the broker handshake.

Existing election tests are deleted, not ported.

## Migration / Rollout

1. Land the `PostMessageEndpoint` abstraction in TS and the Rust bridge. This is the same plumbing the previous spec needed. Keep the current dedicated-worker path working.
2. Land the protocol changes: `request_id`, `session_id`, `LeaderMigrated`, per-peer ack fields. Cover with worker-host unit tests before any broker work.
3. Add `jazz-shared-worker.ts` broker. Stand it up behind a build flag so it can be exercised in isolation.
4. Add `shared-worker-supervisor.ts` lock acquisition and worker-lifecycle handling. Cover with lock-backend unit tests.
5. Wire `Db` to use the broker port instead of the in-tab dedicated worker, behind a feature flag. CI runs both paths during rollout.
6. Run the capability matrix on Chrome, Safari, Firefox, Edge, and supported WebViews. Confirm `navigator.locks`, `SharedWorker`, and dedicated-worker OPFS sync handles.
7. Flip the default to leader-tab persistent mode. Keep the legacy dedicated-worker-per-tab + election path for one release as an emergency rollback.
8. Delete the election machinery and follower-routing code.
9. Update `specs/status-quo/browser_adapters.md` to describe the new topology.

## Open Questions

- **Voluntary handoff policy.** When does a backgrounded leader release its lock? Tunable, but we need a default that doesn't ping-pong when a user alt-tabs.
- **Streaming subscriptions across handoff.** One-shot RPC retry is straightforward; streaming subscriptions need cursor-based resume so the new leader can pick up without replaying. Confirm the existing subscription cursor is sufficient.
- **Idempotency at the leader.** A retried `LeaderMigrated` call may have been partially applied on the previous leader (durable batch admitted but not acked). Use the durable batch id as the dedupe key; spec the exact rules.
- **Broker availability during leader transition.** Browser may terminate an idle `SharedWorker`. Confirm a new connect from any tab wakes it and that pending leader-side `register` calls survive a wake.
- **DevTools ergonomics.** "Which tab is leader" must be discoverable in dev builds. Surface it via a `jazz-devtools:leader` BroadcastChannel or worker name.
- **Memory pressure.** Followers no longer have a persistent worker, which is a win. Measure leader-tab memory under N followers driving traffic.

## References

- `specs/status-quo/browser_adapters.md` — current topology (to be revised after this lands)
- `packages/jazz-tools/src/runtime/db.ts` — current persistent-mode entry points and follower routing
- `packages/jazz-tools/src/worker/jazz-worker.ts` — current dedicated worker host; becomes the leader-tab runtime host
- `crates/jazz-wasm/src/worker_bridge.rs`, `crates/jazz-wasm/src/worker_host.rs`, `crates/jazz-wasm/src/worker_protocol.rs` — protocol additions needed for `request_id`, `session_id`, `LeaderMigrated`
- WHATWG File System Standard: `FileSystemSyncAccessHandle` is `[Exposed=DedicatedWorker]` — the reason the runtime cannot live in the SW
- MDN: `Web Locks API`, `SharedWorker`, `BroadcastChannel`
- Roy Hashimoto, "Sharing OPFS access across browser tabs" — origin of the leader-tab + port-broker pattern
