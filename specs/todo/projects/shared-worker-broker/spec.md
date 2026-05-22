# SharedWorker Broker — Leader Election & Worker Liveness

Status: design-phase. Not implemented.

## What this replaces

Today's cross-tab coordination uses `runtime/tab-leader-election.ts` (BroadcastChannel + `navigator.locks`, lock held by the **tab**) plus `runtime/tab-sync-protocol.ts` (BroadcastChannel data plane). The election logic is correct but has two weaknesses:

1. The lock is held by the tab's main thread. A frozen or unresponsive tab can keep holding the lock long after its Worker is effectively dead (or vice versa — the Worker can die while the tab still holds the lock).
2. BroadcastChannel data sync fans every message out to every tab. Per-pair back-pressure is impossible and the followers do redundant work.

This spec moves to a **SharedWorker broker** that owns election state, plus a **one-lock-held-by-the-Worker** liveness signal. Data sync moves from BroadcastChannel to per-pair MessagePorts minted by the broker. Follower tabs route their main-thread runtime traffic over a lightweight MessagePort transport; the leader Worker attaches those ports directly to the existing Rust peer-client routing table.

## Goals

1. Exactly one tab — the leader — owns the dedicated Worker, OPFS write handle, and upstream server connection at any time.
2. Worker liveness is observable by any tab via a single `navigator.locks` name, regardless of which tab spawned the Worker.
3. Election survives leader-tab close, leader-worker crash, and SharedWorker eviction. Under BFCache/frozen-leader cases, v1 preserves single-writer safety and surfaces degraded liveness instead of stealing the lock by default.
4. Followers do not poll. They get woken by a port message when state changes.
5. The leader tab's main thread is not on the steady-state follower data path. It only forwards broker-minted ports into the Worker once.
6. No changes to storage format, sync semantics, OPFS layout, schema/query behavior, or server protocol. Rust changes are limited to worker-host/protocol plumbing so `MessagePort`s can attach to the existing peer-client routing table under synthetic `tab:<uuid>` peer ids.

## Non-goals

- Multi-leader / sharded writes. One writer, period.
- Cross-origin coordination. SharedWorker is partitioned by origin and that's exactly what we want.
- Replacing the BroadcastChannel fallback for browsers without SharedWorker. The fallback stays as a strategy.
- Rewriting the core WASM runtime sync model. The new Rust work is boundary plumbing, not a new database/sync engine.

## Topology

```text
┌─────────────────────────────────────────────────────────────────────┐
│   Tab A (leader)  ─ same entity as "Tab A" in the bottom row        │
│   ┌────────────────────┐                                            │
│   │ main WasmRuntime   │                                            │
│   └─────────┬──────────┘                                            │
│             │ existing WorkerBridge for the leader's own main       │
│             │ runtime + one-time broker port handoff per follower   │
│             ▼                                                       │
│         ┌────────────────┐                                          │
│         │ Worker         │ ─ holds navigator.locks `LOCK_NAME`      │
│         │ Rust host      │   owns OPFS write handle + upstream WS   │
│         │ + port mux     │   owns one data-plane MessagePort per    │
│         └────────────────┘   follower after the handoff             │
└─────────────────────────────────────────────────────────────────────┘
                ╎
                ╎ Web Lock `LOCK_NAME`  (shared identity, not a transport;
                ╎                        Worker holds, broker watches the
                ╎                        same lock name for failover)
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│   SharedWorker (Broker)                                             │
│     - leader state (phase, leaderTabId, tabs map)                   │
│     - watches LOCK_NAME for failover                                │
│     - mints MessageChannels; the leader-side end is transferred     │
│       through the leader's main thread into its Worker, the other   │
│       end is shipped to the follower after the Worker acks attach   │
└───────┬──────────────────┬─────────────────┬────────────────────────┘
        ▲                  ▲                 ▲      control plane
        │                  │                 │      (tab ↔ broker,
        ▼                  ▼                 ▼      bidirectional)
    ┌───┴──┐           ┌───┴──┐           ┌──┴───┐
    │Tab A │           │Tab B │           │Tab C │
    └──────┘           └──────┘           └──────┘
    (leader,           (follower)         (follower)
     same as
     box above)

   Data plane, steady state (one bidirectional MessagePort per follower):

       Tab B (main) ◄──── MessagePort ────► Tab A Worker
       Tab C (main) ◄──── MessagePort ────► Tab A Worker

   The leader's main thread is not on the steady-state data path.
   It participates only once at port-attach time, transferring the
   broker-minted port into its Worker. From then on, follower sync
   payloads flow directly into the Worker's Rust-side port multiplexer,
   which fans them into the existing WASM peer plumbing under peer-id
   "tab:<follower-uuid>" (same PeerOpen / PeerSync / PeerClose
   semantics, reached through a new worker-host attach path).
   Followers have no dedicated Worker of their own; they only spawn
   one if and when they win an election.
```

Transports:

- **Control plane** (every tab ↔ broker): SharedWorker connection port. Election messages only. Bidirectional. Every tab — leader and followers — has exactly one.
- **Follower main transport** (follower main runtime ↔ follower data-plane port): a lightweight main-runtime transport that installs the same kind of server-bound outbox forwarding that `WorkerBridge` installs today, but without spawning a dedicated Worker.
- **Data plane** (follower main ↔ leader Worker, one per follower): a `MessageChannel` minted by the broker. The leader-side end is transferred through the leader's main thread into its Worker at attach time; steady-state messages flow directly between follower main and leader Worker without touching leader main. Carries existing `tab-sync-protocol.ts` payloads. Bidirectional. Replaces the BroadcastChannel data plane.
- **Worker bridge** (leader main ↔ leader Worker): existing `WorkerBridge` postMessage plumbing for the leader's own main runtime, lifecycle, init/shutdown, auth, and tests. The one-time follower-port handoff uses a new worker-host JS-object message because `MessagePort` cannot be postcard-encoded.
- **Web Lock `LOCK_NAME`** (Worker holds, broker observes): liveness signal, not a transport.

The BroadcastChannel fallback path (for browsers without SharedWorker) is intentionally omitted from this diagram; see "Non-goals".

## Broker identity & bundling

There is exactly one logical broker per `(origin, appId, dbName)`. A broker never coordinates multiple database names, and multiple `(appId, dbName)` pairs in one origin intentionally create separate `SharedWorker` instances.

SharedWorker identity is browser-defined as `(origin, script URL, name)`, so the strategy must control both URL resolution and `name`:

```ts
BROKER_SCOPE = stableScopeId(appId, dbName); // canonicalized/hash-safe, no raw user data requirement
BROKER_NAME = `jazz-worker-broker:${BROKER_SCOPE}`;
BROKER_PROTOCOL_VERSION = 1;

const broker = new SharedWorker(resolveSharedWorkerBrokerUrl(runtimeSources), {
  type: "module",
  name: BROKER_NAME,
});
```

`BROKER_NAME` carries the `(appId, dbName)` identity. The script URL identifies the deployed broker code, not the database scope.

URL resolution order:

1. `runtimeSources.sharedWorkerBrokerUrl`, if provided by the consumer. This is the preferred production escape hatch for apps that want a stable, non-content-hashed broker URL.
2. `runtimeSources.baseUrl` plus a package-defined broker asset path.
3. A bundler-emitted static URL, e.g. `new URL("./broker/shared-worker-broker.js", import.meta.url)`, mirroring the existing dedicated Worker URL pattern.

The implementation must document that content-hashed broker URLs can create more than one physical `SharedWorker` instance during a rolling deploy. That is acceptable only because the Web Lock below remains the version-independent writer safety boundary.

## Lock

One name, fixed per `(appId, dbName)`:

```
LOCK_NAME = `jazz-worker:${appId}:${dbName}`
```

`LOCK_NAME` is deliberately **not** versioned. Different Jazz package versions that point at the same OPFS database must contend on the same lock. Versioning the lock without also versioning the OPFS namespace would allow two writers to the same storage.

**Worker** acquires it on boot:

```ts
navigator.locks.request(LOCK_NAME, { mode: "exclusive" }, () => new Promise(() => {})); // never resolves; lock held until termination
```

The Worker must acquire `LOCK_NAME` before opening the persistent runtime, OPFS sync access handles, or upstream WebSocket. The browser releases the lock automatically when the Worker context terminates (parent tab closed, Worker crashed, Worker called `close()`, hard-kill).

**Broker** uses two patterns against the same name:

- _Probe_ on demand: `navigator.locks.request(LOCK_NAME, { mode: "exclusive", ifAvailable: true }, async lock => { ... })`. `lock === null` → held → Worker alive. `lock !== null` → free → release immediately and start election.
- _Failover watch_ while in `leading`: a queued `navigator.locks.request(LOCK_NAME, ...)` that fires the moment the lock becomes free. Callback releases immediately and triggers re-election.

The same lock release event covers Worker crash, leader-tab close, and OPFS handoff: the Worker held the lock, the browser released it on termination, OPFS sync access handles are gone too. The Web Lock is the unified liveness signal.

## Cross-version safety

Rolling deploys can put two tabs from different Jazz versions on the same origin. If the versions resolve different broker script URLs, the browser can create two physical `SharedWorker` brokers even when both use the same `BROKER_NAME`. This is a new deployment risk compared with BroadcastChannel.

Safety rule: multiple brokers may exist, but multiple writers must not. The version-independent `LOCK_NAME` is the shared exclusion boundary across all broker script URLs and all Jazz package versions for the same `(appId, dbName)`.

Required behavior:

- Every control-plane `HELLO` includes `brokerProtocolVersion`, `jazzPackageVersion`, and `scope = BROKER_SCOPE`.
- A broker rejects tabs whose `scope` does not match its own scope.
- A broker rejects tabs whose `brokerProtocolVersion` is not compatible with its own. Rejection is explicit `BROKER_FAULT` with reason `version-mismatch`; do not silently fall back to BroadcastChannel for the same persistent database while a SharedWorker-capable strategy is active.
- A broker probes `LOCK_NAME` before issuing `YOU_ARE_LEADER`. If the lock is held by an unknown owner or an incompatible-version leader, the broker must not elect a new leader. It reports `BROKER_FAULT` with reason `unknown-lock-owner` or `version-mismatch` and waits for the lock to release.
- A candidate tab that receives `STAND_DOWN` before `LEADER_READY` must terminate or abort its candidate Worker. This prevents a stale candidate from acquiring `LOCK_NAME` after the broker has moved on.
- The Worker acquisition order is lock first, OPFS/upstream second. A candidate that cannot acquire `LOCK_NAME` never opens persistent storage.

Stable consumer-hosted broker URLs reduce the chance of split brokers during a deploy, but correctness must not rely on that. The lock and version checks are the safety mechanism.

## Broker state

```ts
type Phase = "no-leader" | "electing" | "leading";

interface TabRecord {
  id: TabId; // uuid generated tab-side, sent in HELLO
  port: MessagePort; // the SharedWorker.onconnect port
  bornAt: number; // monotonic, ordering for tiebreak
}

interface BrokerState {
  phase: Phase;
  generation: number; // increments every time broker sends YOU_ARE_LEADER
  tabs: Map<TabId, TabRecord>;
  leader: TabId | null;
  candidate: TabId | null; // set during "electing"
  pendingWhoIsLeader: TabId[]; // tabs awaiting a leader/PEER_PORT
  pendingAttachRequests: Map<
    TabId,
    {
      leader: TabId;
      generation: number;
      followerPort: MessagePort; // held until leader Worker acks attach
      startedAt: number;
    }
  >;
}
```

Transitions:

| From        | Event                                            | To          | Side-effect                                                                                                                                                                          |
| ----------- | ------------------------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| any         | tab `onconnect`                                  | same        | add to `tabs` after scope/protocol validation; if phase=leading, mint a port for the new follower                                                                                    |
| `no-leader` | `WHO_IS_LEADER` from tab T                       | `electing`  | probe `LOCK_NAME`; if free, increment `generation`, `candidate=T`, send `YOU_ARE_LEADER(generation)`, arm `ELECTION_TIMEOUT`; if held by unknown owner, emit `BROKER_FAULT` and wait |
| `no-leader` | further `WHO_IS_LEADER` from tab U               | `electing`  | push U to `pendingWhoIsLeader`                                                                                                                                                       |
| `electing`  | `LEADER_READY` from `candidate`                  | `leading`   | `leader=candidate`; mint ports for every pending follower; arm generation-scoped failover lock watch                                                                                 |
| `electing`  | `ELECTION_TIMEOUT`                               | `electing`  | evict `candidate` (`STAND_DOWN`, terminate/abort candidate Worker); pick next from `tabs`; increment `generation`; retry with fresh `YOU_ARE_LEADER`, up to N                        |
| `electing`  | candidate `GOODBYE` or control heartbeat timeout | `electing`  | same as ELECTION_TIMEOUT; retry uses a fresh generation                                                                                                                              |
| `leading`   | leader `GOODBYE` or control heartbeat timeout    | `no-leader` | drop `leader`; broadcast STAND_DOWN; immediately probe lock and re-elect from remaining tabs                                                                                         |
| `leading`   | failover lock watch fires                        | `no-leader` | if watch generation is current: release lock callback; broadcast STAND_DOWN; re-elect                                                                                                |
| `leading`   | new tab sends WHO_IS_LEADER                      | `leading`   | broker mints `MessageChannel`; sends leader-side port to leader as `ATTACH_FOLLOWER_PORT`; after `ATTACH_PORT_ACK`, forwards follower-side port as `PEER_PORT`                       |
| any         | tab `GOODBYE` or control heartbeat timeout       | same        | remove from `tabs`; if it was leader, see above                                                                                                                                      |

`bornAt` is for deterministic tiebreaks. When picking a new leader from `tabs`, the broker chooses the oldest survivor — biases toward already-attached tabs that have likely warmed their main-thread runtimes.

Every leader-scoped message carries `generation`. The broker ignores stale `LEADER_READY`, `ATTACH_PORT_ACK`, `ATTACH_PORT_FAILED`, and failover-watch callbacks whose generation no longer matches `state.generation`. Followers drop any data-plane port whose `leaderTabId` or `generation` no longer matches their current election snapshot.

Generation rule: every `YOU_ARE_LEADER` issuance gets a new generation, including retries inside the same visible election episode. Two different candidates must not share a generation. Initial broker state starts at `generation = 0`; the first candidate receives generation `1`.

## Control-plane messages (Broker ↔ Tab)

```ts
type TabToBroker =
  | {
      t: "HELLO";
      tabId: TabId;
      bornAt: number;
      scope: string; // must equal BROKER_SCOPE
      brokerProtocolVersion: number; // must be compatible with broker
      jazzPackageVersion: string;
      current?: {
        role: "leader" | "follower";
        leaderTabId: TabId | null;
        generation: number | null;
        hasLeaderWorker: boolean;
      };
    }
  | { t: "WHO_IS_LEADER" }
  | { t: "LEADER_READY"; generation: number } // sent by elected tab AFTER its Worker holds the lock
  | { t: "ATTACH_PORT_ACK"; forTab: TabId; generation: number } // leader-only; sent after Worker acks attach
  | { t: "ATTACH_PORT_FAILED"; forTab: TabId; generation: number; reason: string } // leader-only
  | { t: "FOLLOWER_PORT_CLOSED"; leaderTabId: TabId; generation: number } // explicit cleanup; best-effort
  | { t: "HEARTBEAT"; tabId: TabId }
  | { t: "GOODBYE" };

type BrokerToTab =
  | { t: "YOU_ARE_LEADER"; generation: number }
  | {
      t: "ATTACH_FOLLOWER_PORT";
      forTab: TabId;
      leaderTabId: TabId;
      generation: number;
      port: MessagePort;
    } // sent only to leader
  | { t: "PEER_PORT"; port: MessagePort; leaderTabId: TabId; generation: number }
  | { t: "STAND_DOWN"; generation: number; reason: "leader-lost" | "stale" | "broker-fault" }
  | { t: "HEARTBEAT_ACK" }
  | {
      t: "BROKER_FAULT";
      generation: number;
      reason:
        | "version-mismatch"
        | "scope-mismatch"
        | "unknown-lock-owner"
        | "leader-unavailable"
        | "election-failed"
        | string;
    };
```

All transferable ports use `postMessage(msg, [port])`. Correctness does not depend on `MessagePort` `"close"` events: ports are cleaned up through explicit `GOODBYE` / `FOLLOWER_PORT_CLOSED`, control heartbeat timeout, generation changes, `STAND_DOWN`, and attach timeouts. `messageerror` can be logged, but it is diagnostic only. Heartbeats are for tab/broker liveness, not leader discovery polling.

## Data-plane messages (Follower ↔ Leader)

Reuse `runtime/tab-sync-protocol.ts` payloads verbatim. The transport is the `MessageChannel` minted by the broker; semantics unchanged.

### Peer-id namespace

Tab peers are registered with the worker host under the namespaced id `tab:<uuid>`. This is a committed decision, not an open question:

- The existing `PeerOpen / PeerSync / PeerClose` namespace (backed by `PEER_ROUTING.peer_client_by_peer_id` in `crates/jazz-wasm/src/worker_host.rs`) treats `peer_id: &str` as an opaque `HashMap` key. No format constraints exist in the Rust layer, so the choice of separator is unconstrained.
- That namespace is, today, owned exclusively by the tab-sync layer: the only call site of `openPeer` is `packages/jazz-tools/src/runtime/db.ts:1302`, which currently passes a raw tab UUID with no prefix. Network sync-server connections go through a different code path (`wss://` plumbing in `worker_host.rs`) and never enter `PEER_ROUTING`.
- Under this design, the per-message follower-sync path moves out of `db.ts` (main thread) and into the leader's Worker. The `db.ts:1302` call site is removed; the equivalent peer open/sync/close operations happen inside `worker_host.rs` against the same `PEER_ROUTING` table. The namespace boundary is unchanged.
- Adopting `tab:<uuid>` therefore introduces the first explicit namespace boundary into a previously-flat namespace. It is forward-compatible with any future non-tab peer kinds (e.g. `net:<host>` for cross-origin relays) and adds no risk over the status quo.

This satisfies Goal 6 because the core runtime still sees ordinary peer clients. The change is at the worker-host boundary: a `MessagePort` becomes another peer transport target.

### Port-attach flow (leader side)

The broker mints a `MessageChannel` and ships one end to the leader's main thread. Main immediately transfers it into its Worker. The broker does **not** ship the follower-side port until the Worker acks that the leader-side port is attached; this prevents the follower from sending into a port with no registered peer.

```ts
// broker-side pseudocode
function attachFollowerToLeader(followerTabId: TabId) {
  const mc = new MessageChannel();
  const generation = state.generation;
  state.pendingAttachRequests.set(followerTabId, {
    leader: state.leader!,
    generation,
    followerPort: mc.port2,
    startedAt: now(),
  });

  postToLeader(
    {
      t: "ATTACH_FOLLOWER_PORT",
      forTab: followerTabId,
      leaderTabId: state.leader!,
      generation,
      port: mc.port1,
    },
    [mc.port1],
  );
}
```

Leader main forwards the port once and waits for the Worker-host ack:

```ts
// leader main-thread handler for ATTACH_FOLLOWER_PORT
function handleAttachFollowerPort(msg: AttachFollowerPort) {
  worker.postMessage(
    {
      type: "attach-follower-port",
      followerTabId: msg.forTab,
      leaderTabId: msg.leaderTabId,
      generation: msg.generation,
    },
    [msg.port],
  );
  // WorkerBridge receives a worker-host ack/failure and broker-client
  // forwards ATTACH_PORT_ACK / ATTACH_PORT_FAILED to the broker.
}
```

Inside the Worker, `worker_host.rs` handles this JS-object message separately from the postcard `MainToWorkerWire` path because transferred `MessagePort`s are available on `MessageEvent.ports`, not in `event.data`:

```rust
// pseudocode, Rust worker-host attach path
on_message(event) {
  if event.data.type == "attach-follower-port" {
    let port = event.ports[0];
    let peer_id = format!("tab:{}", event.data.followerTabId);
    ensure_peer_client(runtime, &peer_id);
    PEER_ROUTING.register_port(peer_id, event.data.generation, port);
    post_to_main(WorkerToMainWire::FollowerPortAttached { follower_tab_id, generation });
    return;
  }

  // Existing postcard MainToWorkerWire path.
}
```

For follower → leader traffic, the port handler accepts existing `FollowerSyncMessage` objects, validates `toLeaderTabId` and `term` against the attached port's leader/generation record, then routes `payload` through the same code path used by `MainToWorkerWire::PeerSync`.

For leader → follower traffic, the worker-side outbox already resolves client-bound messages through `PEER_ROUTING`. Extend that lookup to return an attached `MessagePort` when one exists. If a port is present, the Worker posts an existing `LeaderSyncMessage` object directly to that port. If no port exists, it falls back to the current `WorkerToMainWire::PeerSync` path so tests and the BroadcastChannel strategy keep working during migration.

### Follower main transport

Follower tabs no longer spawn a dedicated Worker just to get a `WorkerBridge` sync sender. Instead, `broker-client` installs a lightweight runtime transport on the follower's main `WasmRuntime` when it receives `PEER_PORT`.

That transport does two things:

1. Server-bound outbox from the follower main runtime is forwarded to the port as existing `FollowerSyncMessage` objects.
2. Incoming `LeaderSyncMessage` payloads from the port are applied to the follower main runtime via `WasmRuntime.onSyncMessageReceived(payload, sequence?)`, the same underlying API that `WorkerBridge.applyIncomingServerPayload` uses today.

On install, the transport must also replay the main runtime's server edge (`removeServer`/`addServer`) just like `WorkerBridge.replayServerConnection()` does today. That gives the main runtime somewhere to route server-bound outbox entries even though the physical upstream connection lives in the leader Worker.

Implementation-wise this can reuse/factor the existing `RustOutboxSender` machinery instead of inventing a second sender model. The important boundary is that the follower main runtime gets a sender and a replayed server edge even when `Db.spawnWorker(...)` is skipped.

## Sequence flows

### 1. Cold start

```
Tab A connects to SharedWorker named BROKER_NAME for (appId, dbName)
Tab A → broker: HELLO(scope, brokerProtocolVersion, jazzPackageVersion); WHO_IS_LEADER
broker: probe lock → free → no Worker exists
broker: phase=electing, candidate=A
broker → Tab A: YOU_ARE_LEADER(generation=1)
Tab A: createPersistentDb() → spawns Worker → bridge.init()
Worker (before OPFS/upstream): navigator.locks.request(LOCK_NAME, ...) HELD
Worker → Tab A: init-ok
Tab A → broker: LEADER_READY(generation=1)
broker: phase=leading, leader=A
broker: arms failover lock watch
broker: drains pendingWhoIsLeader (empty here)
```

### 2. Follower joins

```
Tab B connects to the same BROKER_NAME
Tab B → broker: HELLO(scope, brokerProtocolVersion, jazzPackageVersion); WHO_IS_LEADER
broker: const mc = new MessageChannel()
broker → Tab A: ATTACH_FOLLOWER_PORT(forTab=B, generation=1, port=mc.port1)
Tab A (main): worker.postMessage(
                { type: "attach-follower-port", followerTabId: "B", leaderTabId: "A", generation: 1 },
                [mc.port1])             // hand the leader-side port to the Worker
Tab A Worker: receives port; registers it in port-mux as peer-id "tab:B";
              ensures peer client "tab:B" in PEER_ROUTING
Tab A Worker → Tab A main: follower-port-attached(forTab=B, generation=1)
Tab A (main) → broker: ATTACH_PORT_ACK(forTab=B, generation=1)
broker → Tab B: PEER_PORT(port=mc.port2, leaderTabId=A, generation=1)
Tab B (main): attaches mc.port2 to its tab-sync handler
              → follower-sync handshake runs as today;
                payloads flow directly between Tab B main and Tab A Worker
```

### 3. Leader-tab close (graceful or hard)

```
Tab A closes → Worker terminated → LOCK_NAME released
broker: failover lock watch fires → callback releases the lock
broker: phase=no-leader, leader=null
broker → Tab B, Tab C: STAND_DOWN
broker: pick next leader (oldest survivor, e.g. B)
broker: phase=electing, candidate=B
broker → Tab B: YOU_ARE_LEADER(generation=2)
Tab B: tears down follower main transport, spawns Worker → lock acquired → LEADER_READY(generation=2)
broker: phase=leading
broker → Tab C: PEER_PORT(new mc, leaderTabId=B, generation=2)
```

Followers' main-thread runtimes never go away. Their un-acked writes survive the handover.

### 4. Worker crashes but leader tab survives

```
Worker dies (OOM, native crash, manual terminate)
LOCK_NAME released (browser auto-release on Worker context termination)
broker: failover lock watch fires
broker → Tab A (the now-ex-leader): STAND_DOWN
broker: re-elect (Tab A may itself win again — gets fresh YOU_ARE_LEADER and respawns Worker)
```

### 5. Elected leader never sends LEADER_READY

```
broker armed ELECTION_TIMEOUT (3s) when issuing YOU_ARE_LEADER(generation=N)
Timeout fires before LEADER_READY(generation=N) arrives.
broker → candidate: STAND_DOWN(generation=N, reason="stale")
candidate terminates/aborts its Worker if one was started
broker picks next from tabs; retries with YOU_ARE_LEADER(generation=N+1).
After N retries (3), broker emits BROKER_FAULT to all tabs and halts election.
Tabs surface this as an error from createDb().
```

### 6. SharedWorker eviction (last tab navigates away then a new one opens)

```
Last tab closes → SharedWorker is GC'd by the browser (lifecycle spec).
New tab opens → spawns a fresh SharedWorker.
The fresh broker probes LOCK_NAME: free (no Worker held it across the gap).
Cold start (case 1).
```

### 7. Broker restart while tabs remain

Browsers should keep a SharedWorker alive while connected ports exist, but the implementation must tolerate a broker process restart or script reload.

```
Broker restarts → all existing control ports disappear / error / stop responding
Each tab's broker-client reconnects and sends HELLO with current local state:
  { scope, brokerProtocolVersion, jazzPackageVersion, tabId, bornAt, role, leaderTabId, generation, hasLeaderWorker }
Fresh broker probes LOCK_NAME.
Fresh broker sets generation = max(0, ...HELLO.current.generation values it accepts).
If lock is free: normal election; next candidate receives generation=maxSeen+1.
If lock is held and exactly one reconnected tab reports hasLeaderWorker=true:
  broker adopts that tab as leader for the reported generation after a health ping
  broker remints follower ports for all other tabs
If lock is held but no tab claims the Worker:
  broker reports BROKER_FAULT/unknown-lock-owner and waits for lock release
If any tab reports an incompatible brokerProtocolVersion:
  broker rejects that tab with BROKER_FAULT/version-mismatch
```

The broker must not elect a new leader while `LOCK_NAME` is held by an unknown Worker. That would create a second writer if the old Worker is still alive. If a leader is adopted after restart, the broker keeps the adopted generation; it only increments when it issues a fresh `YOU_ARE_LEADER`.

### 8. BFCache freeze of leader tab

This is the messy case. iOS Safari aggressively BFCaches inactive tabs. A frozen tab cannot service `ATTACH_FOLLOWER_PORT` requests. Existing follower ports only keep working if the browser keeps the leader Worker running; if the Worker is frozen with the tab, the data plane stalls too. The Web Lock tells us the Worker still holds authority, but not whether it can make progress. We cannot distinguish "frozen" from "alive but slow" purely from the lock.

Safety-first default: do **not** steal the lock automatically. Instead:

```
leader pagehide/freeze hint → leader main asks Worker to flush WAL and voluntarily shutdown/stand down when possible
Worker shutdown → LOCK_NAME released
broker failover watch fires → normal re-election

if a new follower joins while the leader is frozen and cannot attach a port:
broker times out ATTACH_FOLLOWER_PORT
broker returns BROKER_FAULT/leader-unavailable to that follower instead of creating a second writer
```

Liveness-first lock stealing is a possible later mode, but it must be explicit and browser-gated:

```
broker → leader port: FORCE_RELEASE  (best-effort; ignored if frozen)
broker waits grace period
broker requests LOCK_NAME with { steal: true }
broker emits STAND_DOWN, increments generation, re-elects
old Worker, on resume, queries navigator.locks.query(); if LOCK_NAME is no longer held by it, it shuts down before writing
```

Cost: lock stealing optimizes liveness over strict "one live OPFS owner" safety. It must stay disabled until Safari/WebKit behavior is verified with a real browser spike. The implementable v1 should ship the safety-first path and mark BFCache failover as degraded rather than solved.

## File-by-file impact

| File                                                                               | Change                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Risk                                                                                                             |
| ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **NEW** `packages/jazz-tools/src/runtime/broker/protocol.ts`                       | Typed `TabToBroker` / `BrokerToTab` enums + helper builders.                                                                                                                                                                                                                                                                                                                                                                                                                        | low                                                                                                              |
| **NEW** `packages/jazz-tools/src/runtime/broker/shared-worker-broker.ts`           | The SharedWorker entry script. Owns `BrokerState`, scope/protocol validation, lock watch, port minting. Bundled as a separate worker entry.                                                                                                                                                                                                                                                                                                                                         | medium — new code, but small; testable in isolation.                                                             |
| **NEW** `packages/jazz-tools/src/runtime/broker/broker-client.ts`                  | Tab-side facade. Resolves `SharedWorker` URL/name, sends scoped/versioned `HELLO`, exposes `connect()`, `claimLeadership()`, `onLeaderPortChanged(cb)`, `handleAttachFollowerPort(msg)` (transfers broker-minted leader-side port into the Worker, forwards Worker ack/failure), and follower `PEER_PORT` lifecycle.                                                                                                                                                                | medium                                                                                                           |
| **NEW** `packages/jazz-tools/src/runtime/broker/message-port-runtime-transport.ts` | Follower main-runtime transport. Installs server-bound forwarding onto the follower main `WasmRuntime`, replays the runtime server edge, posts `FollowerSyncMessage` to `PEER_PORT`, applies incoming `LeaderSyncMessage` via `onSyncMessageReceived`.                                                                                                                                                                                                                              | medium                                                                                                           |
| `packages/jazz-tools/src/runtime/context.ts`                                       | Extend `RuntimeSourcesConfig` with optional `sharedWorkerBrokerUrl` for consumer-hosted stable broker assets.                                                                                                                                                                                                                                                                                                                                                                       | low                                                                                                              |
| `packages/jazz-tools/src/runtime/runtime-config.ts`                                | Add `resolveRuntimeConfigSharedWorkerBrokerUrl(...)`, mirroring worker URL resolution with `sharedWorkerBrokerUrl`, `baseUrl`, and static `new URL(...)` fallback.                                                                                                                                                                                                                                                                                                                  | low                                                                                                              |
| `packages/jazz-tools/src/runtime/tab-leader-election.ts`                           | Refactor into a strategy interface. `SharedWorkerBrokerStrategy` (new) and `BroadcastChannelStrategy` (existing, kept as fallback). Feature-detect `typeof SharedWorker !== "undefined"`.                                                                                                                                                                                                                                                                                           | medium                                                                                                           |
| `packages/jazz-tools/src/runtime/leader-lock.ts`                                   | Lock acquisition moves out of the tab. File becomes a thin helper called from `jazz-worker.ts`.                                                                                                                                                                                                                                                                                                                                                                                     | low                                                                                                              |
| `packages/jazz-tools/src/runtime/tab-sync-protocol.ts`                             | Payload shapes unchanged. Transport abstraction: introduce a `Transport` interface implemented by both `BroadcastChannelTransport` (existing) and `MessagePortTransport` (new).                                                                                                                                                                                                                                                                                                     | low–medium                                                                                                       |
| `packages/jazz-tools/src/runtime/storage-reset-coordinator.ts`                     | Route reset coordination through the selected transport/broker fanout instead of assuming BroadcastChannel fanout.                                                                                                                                                                                                                                                                                                                                                                  | medium                                                                                                           |
| `packages/jazz-tools/src/runtime/db.ts`                                            | (a) Followers must not spawn a dedicated Worker; gate `Db.spawnWorker(...)` on leader role. (b) Install/tear down `message-port-runtime-transport` when follower receives/drops `PEER_PORT`. (c) Remove the BroadcastChannel-based `handleFollowerSync` path (currently around `db.ts:1302`): leader-side per-message handling now lives in the Worker. (d) Wire `broker-client` events into the existing election state machine (`adoptLeaderSnapshot`, `onLeaderElectionChange`). | high — touches the existing leader/follower control flow, worker-spawn gate, and main-runtime sync sender setup. |
| `packages/jazz-tools/src/worker/jazz-worker.ts`                                    | On boot, request `LOCK_NAME` exclusive (never resolve). Surface a `lockAcquired` signal before `init-ok` so the leader tab sends `LEADER_READY` only after the lock is real. The port multiplexer itself lives in Rust after `runAsWorker()` takes over.                                                                                                                                                                                                                            | low–medium                                                                                                       |
| `packages/jazz-tools/src/runtime/worker-bridge.ts`                                 | Add listener surface for Worker-host follower-port attach ack/failure so `broker-client` can forward `ATTACH_PORT_ACK` / `ATTACH_PORT_FAILED` to the broker.                                                                                                                                                                                                                                                                                                                        | low–medium                                                                                                       |
| `crates/jazz-wasm/src/worker_protocol.rs`                                          | Add typed worker-host messages for follower-port attach ack/failure. Do **not** postcard-encode `MessagePort`; attach requests are JS-object messages read from `MessageEvent.data` plus `MessageEvent.ports`.                                                                                                                                                                                                                                                                      | medium                                                                                                           |
| `crates/jazz-wasm/src/worker_bridge.rs`                                            | Parse new Worker → main attach ack/failure messages and expose optional listener slots to TypeScript.                                                                                                                                                                                                                                                                                                                                                                               | low–medium                                                                                                       |
| `crates/jazz-wasm/src/runtime.rs`                                                  | Factor/reuse `RustOutboxSender` for follower main-runtime `MessagePort` transport and extend peer routing output so worker-side peer messages can post directly to an attached `MessagePort` when present.                                                                                                                                                                                                                                                                          | medium-high — shared sync-sender code, needs focused tests.                                                      |
| `crates/jazz-wasm/src/worker_host.rs`                                              | Add follower port table, attach handling from `MessageEvent.ports`, explicit cleanup, follower-sync parsing, and direct leader-sync posting to attached ports via existing `PEER_ROUTING`.                                                                                                                                                                                                                                                                                          | high — new browser-facing Rust/wasm boundary code.                                                               |

## Risks & open questions

- **MessagePort transfer through SharedWorker**: spec-compliant but worth a 30-minute standalone playground spike to confirm browser support (Chrome, Firefox, Safari desktop, Safari iOS). Block on this before implementation.
- **SharedWorker URL stability across deploys**: content-hashed broker URLs can create split brokers. The app can opt into `runtimeSources.sharedWorkerBrokerUrl` for URL stability, but tests must prove split broker URLs still cannot create two writers because `LOCK_NAME` is shared.
- **MessagePort transfer from page main → dedicated Worker → Rust wasm closures**: this is the critical new Rust boundary. Spike before implementation with a tiny worker-host prototype that receives a transferred port, installs `onmessage`, and posts back through it.
- **Follower main-runtime transport without WorkerBridge**: today `WorkerBridge` is what installs server-bound outbox forwarding on the main runtime. The new follower transport must reuse/factor that machinery so followers can skip `Db.spawnWorker(...)` without losing sync.
- **`{ steal: true }` semantics on Safari**: only needed for a future liveness-first BFCache mode. V1 should not depend on it.
- **`PEER_PORT` retransmission on leader change**: when a new leader is elected, followers need fresh ports. The broker proactively mints + ships PEER_PORTs to all tabs in `tabs` after `LEADER_READY`. Avoid the "ask again" pattern — eliminates a round trip.
- **Election timeout value (3s)**: workable for normal startup. Cold-Worker boot on a low-end device can exceed this. Make it configurable; default 3000ms, doc'd as "tunable".
- **What surfaces in `createDb()` when broker faults?** Currently the leader-election promise resolves once a port is held. A broker fault must reject with a meaningful error type so app-level fallback logic can intervene (e.g., memory-mode for the duration).
- **Storage reset routing**: `storage-reset-*` messages currently ride the BroadcastChannel sync plane. The MessagePort strategy needs an explicit broker/leader fanout path for reset coordination before BroadcastChannel can be removed from SharedWorker-capable browsers.

## Acceptance / test plan

Playwright multi-context tests (mirroring `e2e/tab-sync.spec.ts` shape):

1. **Cold start.** One tab → it becomes leader, OPFS writes succeed.
2. **Follower join.** Tab B opens, gets a `PEER_PORT`, observes leader writes within X ms.
3. **Leader close.** Tab A closes; Tab B is elected, writes succeed, no data loss for B's pre-handover queued writes.
4. **Worker crash.** Force-terminate the leader's Worker (via `simulateCrash` MessagePort path); broker re-elects the same tab, which respawns its Worker.
5. **Three-tab churn.** A leader, B and C followers. Close A; B becomes leader; close B; C becomes leader. Final read-back from C must be consistent with sum of all writes.
6. **Init-failure retry.** Mock Worker init to fail twice then succeed; broker should re-elect twice then settle.
7. **Attach-failure cleanup.** Leader receives `ATTACH_FOLLOWER_PORT` but Worker never acks; broker closes/drops pending follower port, reports a meaningful error to the follower, and does not leak a peer in `PEER_ROUTING`.
8. **Stale generation ignored.** Delay an `ATTACH_PORT_ACK` from generation N until after generation N+1 leader election; broker must ignore it and follower must keep the newer port.
9. **Follower has no Worker.** In the SharedWorker strategy, a follower writes through the main-runtime MessagePort transport without creating a dedicated Worker until it becomes leader.
10. **Storage reset.** Trigger reset from both leader and follower tabs; all tabs release transports/workers, reset once, and resume consistently.
11. **Scope isolation.** Open two DB names under the same origin. They use different `BROKER_NAME`s and do not share leaders, ports, or lock names.
12. **Stable broker URL override.** Configure `runtimeSources.sharedWorkerBrokerUrl`; `broker-client` uses that exact URL with the scoped `BROKER_NAME`.
13. **Split broker URL safety.** Open two tabs that resolve different broker script URLs but the same `BROKER_NAME` and `LOCK_NAME`; at most one Worker acquires OPFS/upstream, and the other broker reports `unknown-lock-owner` / waits rather than electing.
14. **Version mismatch rejection.** Connect a tab with an incompatible `brokerProtocolVersion`; broker rejects it with `BROKER_FAULT/version-mismatch` and does not fall back to BroadcastChannel for the same persistent database.
15. **Generation retry increments.** Force candidate A to time out, then elect candidate B; B receives generation N+1, and late `LEADER_READY(N)` from A is ignored.
16. **No-SharedWorker fallback.** Run the core leader/follower/churn cases under a `delete globalThis.SharedWorker` shim; the strategy falls back to the existing BroadcastChannel path.
17. **BFCache safety-first.** Marked skipped or browser-specific: verify that attach timeout surfaces `leader-unavailable` rather than stealing the lock.

Each test should run under Chrome and Firefox via Playwright projects.

## Out of scope (for now)

- iframe / Worker-from-iframe nesting.
- Multiple `(appId, dbName)` pairs in one origin sharing a broker. V1 commits to one SharedWorker broker per `(appId, dbName)`.
- Automatic BFCache lock stealing. V1 uses safety-first voluntary stand-down and degraded attach timeout behavior.
- Telemetry around election durations and failover rates — once the system is real, instrument it.

## Glossary

- **Broker** — the SharedWorker singleton for one `(origin, appId, dbName)` scope, keyed by browser identity `(origin, script URL, BROKER_NAME)`.
- **`BROKER_SCOPE`** — stable, canonicalized/hash-safe identifier derived from `(appId, dbName)` and used in `BROKER_NAME` plus `HELLO.scope`.
- **`BROKER_NAME`** — `jazz-worker-broker:${stableScopeId(appId, dbName)}`. Carries the logical database scope in the SharedWorker constructor.
- **`BROKER_PROTOCOL_VERSION`** — integer protocol version for broker/tab control-plane compatibility. Incompatible tabs are rejected explicitly.
- **Leader tab** — the tab that owns the Worker holding `LOCK_NAME`.
- **Follower tab** — any tab that is not the leader; has a data-plane MessagePort to the leader.
- **`LOCK_NAME`** — `jazz-worker:${appId}:${dbName}`. Held by the Worker. Released on Worker termination.
- **Control plane** — tab ↔ broker messages (election).
- **Data plane** — follower ↔ leader messages (existing `tab-sync-protocol.ts`).
