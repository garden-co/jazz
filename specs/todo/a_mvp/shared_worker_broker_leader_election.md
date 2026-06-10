# SharedWorker Broker Leader Election — TODO (MVP)

Replace browser peer leader election with a brokered leader-election model.

The current browser persistent mode elects a leader tab with BroadcastChannel
messages plus a Web Locks lease. It works in normal cases, but under high load
the tab-to-tab election and forwarding path is not reliable enough. This spec
moves cross-tab coordination into a SharedWorker broker, keeps the broker out of
the hot sync-data path, and lets follower data ports terminate inside the
leader's dedicated worker.

## Goals

- Use one SharedWorker broker per `{appId, dbName}` coordination domain.
- Elect exactly one leader tab for a persistent browser database namespace.
- Keep a Web Lock in the leader tab to reduce page freeze risk in Firefox and
  Chromium browsers.
- Keep a separate Web Lock in the leader dedicated worker before OPFS is opened.
- Detect Safari BFCache-style lock loss quickly by monitoring Web Locks from the
  broker.
- Fence every broker instance, leadership ID, and follower port attachment so stale
  tabs/workers cannot continue durable work silently.
- Route follower sync payloads directly to the leader dedicated worker through
  broker-supplied `MessagePort`s.
- Keep the broker protocol small and mostly control-plane.
- Remove the old BroadcastChannel peer election, tab sync, storage reset, and
  fallback worker namespace model.
- Fail fast with an explicit unsupported-environment error when required browser
  APIs are missing.

## Non-goals

- No fallback to the current peer leader-election implementation.
- No follower-tab dedicated workers in persistent browser mode.
- No SharedWorker relay of all Jazz sync payloads.
- No throttling detection or throttling-based demotion in this MVP.
- No change to memory-mode browser clients.
- No public `createDb` or framework API change.

## Required Browser APIs

Persistent browser mode requires all of these APIs:

- `SharedWorker`
- `MessageChannel` / transferable `MessagePort`
- Web Locks in window contexts
- Web Locks in worker contexts
- Web Locks abortable queued requests through `AbortSignal`
- Web Locks `{ steal: true }` for exceptional forced takeover recovery
- OPFS from the dedicated worker persistent runtime path

Initialization must fail before creating Jazz runtimes if the coordination APIs
are unavailable. The error must name the missing capability, for example:

```text
Jazz persistent browser mode requires SharedWorker, MessageChannel, and Web Locks
support. This environment is missing: SharedWorker.
```

If worker-side Web Locks or OPFS fail during the leader-worker preflight, the
failure must also be explicit and must not fall back to peer election or
memory mode.

Persistent browser mode also requires a secure context for Web Locks and OPFS.
The SharedWorker script must be same-origin with the calling page and must be
constructed with stable worker options for the broker script URL/name pair.

## Status Quo To Replace

Current files and concepts to replace:

- `packages/jazz-tools/src/runtime/tab-leader-election.ts`
  - BroadcastChannel election channel: `jazz-leader:<appId>:<dbName>`
  - leader Web Lock: `jazz-leader-lock:<appId>:<dbName>`
- `packages/jazz-tools/src/runtime/tab-sync-protocol.ts`
  - BroadcastChannel tab sync channel: `jazz-tab-sync:<appId>:<primaryDbName>`
  - follower-to-leader and leader-to-follower payload messages
- `packages/jazz-tools/src/runtime/storage-reset-coordinator.ts`
  - BroadcastChannel-based reset coordination
- follower fallback worker namespaces:
  - `${primaryDbName}__fallback__${tabId}`

The new broker must preserve the app-facing behavior while removing these
cross-tab peer coordination paths. It does not acquire the old
`jazz-leader-lock:<appId>:<dbName>` lock. A tab contending with an old-version
leader hangs in the OPFS open retry for up to ~3.4 minutes and then fails with a
generic handle-conflict error; there is no explicit mixed-version detection.

## Architecture

There is one SharedWorker broker per persistent database namespace:

```text
jazz-broker:<appId>:<dbName>
```

The SharedWorker constructor name must use the same coordination domain. The
broker must serve only tabs whose `hello` message matches the broker's namespace
and configuration fingerprint.

The broker owns cross-tab coordination only:

- connected tab registry
- broker instance assignment
- visibility ranking
- leader election
- leadership ID assignment
- tab-lock and worker-lock monitoring
- leader demotion
- follower port assignment
- storage reset coordination

The broker must not parse Jazz sync payloads or become the routine data relay.

Topology:

```text
Tab A / B / C
  -> SharedWorker broker: jazz-broker:<appId>:<dbName>
     -> elects one leader tab

Leader tab
  -> holds jazz-leader-tab:<appId>:<dbName>
  -> spawns dedicated Jazz worker
     -> holds jazz-leader-worker:<appId>:<dbName>
     -> opens primary OPFS namespace
     -> owns persistent runtime + upstream sync

Follower tab
  -> no dedicated worker
  -> main-thread runtime only
  -> receives a MessagePort from the broker
  -> the other port endpoint is transferred into the leader dedicated worker
```

The leader tab's own main-thread runtime keeps the existing direct
`WorkerBridge(worker, runtime)` path to its dedicated worker. Only follower
traffic uses broker-supplied `MessagePort`s.

Follower startup is not considered persistent-worker-ready until:

1. the tab connects to the broker,
2. the broker has a durable-ready leader,
3. the broker supplies a follower data port,
4. the leader dedicated worker acknowledges the follower port.

## Configuration Fingerprint

The first accepted `hello` establishes the broker's configuration fingerprint.
Every later tab connection in that broker instance must match it exactly, or the
broker rejects the tab with `unsupported`.

The fingerprint is a deterministic compatibility value for the durable browser
runtime. It must be stable across short-lived token refreshes but must change
when two tabs cannot safely share one leader worker. It includes at least:

- Jazz package/runtime protocol version
- broker control protocol version
- storage format compatibility version
- schema fingerprint
- `appId`
- primary `dbName`
- persistent driver namespace
- `env`
- `userBranch`
- `serverUrl`
- runtime source identity when custom runtime sources are supplied
- authentication compatibility class and stable identity/admin mode

The fingerprint must not include a transient JWT string or cookie value if the
stable authenticated identity is unchanged. Token refreshes that preserve the
same auth identity are ordinary auth updates; auth changes that move the tab to
a different stable identity/admin mode require a new Db instance and a new
broker participation attempt.

The MVP does not support one leader worker serving multiple schemas, server
URLs, user branches, or auth identities. A mismatch is a hard unsupported
environment/configuration error, not a fallback to peer election or memory mode.

## Election

Every tab has a stable `tabId` for its current page lifetime. On broker
connection, the tab sends `hello` with its `tabId`, `appId`, `dbName`, and a
configuration fingerprint. The broker must reject or disconnect tabs whose
namespace or configuration fingerprint does not match the broker instance.

Tabs report visibility to the broker:

- `visible` when `document.visibilityState === "visible"`
- `hidden` otherwise

Only visible transitions update `lastVisibleAt`.

Leader selection:

1. Prefer connected tabs whose latest visibility state is `visible`.
2. Among visible tabs, choose the tab with the newest `lastVisibleAt`.
3. If no connected tab is visible, choose the connected tab with the newest
   `lastVisibleAt` anyway.

This deliberately accepts possible Safari churn when every tab is hidden. Fast
lock-loss monitoring is the recovery mechanism.

The broker owns leadership IDs. Every promotion increments the leadership ID. Ports and
worker peer attachments are scoped to the leadership ID in which they were created.

The broker also owns a `brokerInstanceId` generated when the SharedWorker broker
starts. Every broker-to-tab control message includes the broker instance. Tabs ignore
messages from stale broker instances and treat broker instance changes as a broker restart.

## Locks

Leadership uses exclusive Web Locks:

- `jazz-leader-tab:<appId>:<dbName>`
  - held by the leader tab
  - exists to keep the page active where Web Locks prevent freezing
  - monitored by the broker
- `jazz-leader-worker:<appId>:<dbName>`
  - held by the leader dedicated worker
  - must be acquired before opening the primary OPFS namespace
  - monitored by the broker

The worker lock is Jazz's coordination lock for the OPFS-backed runtime. The
browser's OPFS `FileSystemSyncAccessHandle` exclusivity still exists, but it
must not be used as the leader-election or handoff mechanism. A worker may open
OPFS only while it holds `jazz-leader-worker:<appId>:<dbName>`, and it must close
OPFS before releasing that lock during clean shutdown.

## Broker Lock Monitoring

After a leader is ready, the broker starts queued lock requests for the tab and
worker lock names. Each monitor request is scoped to the current `brokerInstanceId`,
leadership ID, and lock name. Monitor requests must use an `AbortSignal` so the
broker can cancel them before planned demotion, storage reset, shutdown, or
replacement election.

If a queued monitor request is granted while it still matches the current broker instance
and leadership ID, and the broker has not marked that lock release as planned, the broker
treats that as leader loss:

1. immediately release the broker-acquired lock,
2. mark the current leadership ID dead,
3. stop assigning new follower ports for that leadership ID,
4. instruct connected tabs to close follower data ports for that leadership ID,
5. elect a replacement.

This is the fast path for Safari BFCache behavior: if a hidden leader tab enters
BFCache and loses its tab lock, the broker's queued tab-lock monitor should be
granted and failover can start without waiting for a generic liveness timeout.

The same mechanism applies to worker crashes. If the leader worker dies and its
worker lock is released, the broker's queued worker-lock monitor is granted and
failover starts.

If a monitor is granted for an old broker instance/leadership ID, or after the broker has cancelled
it for a planned transition, the broker releases the lock and ignores the grant.
Normal demotion and reset must not be reported as unexpected lock loss merely
because the queued monitor became grantable.

## Promotion

Promotion flow:

1. Broker chooses a candidate by the visibility ranking.
2. Broker sends `become-leader` with a new leadership ID.
3. Candidate acquires `jazz-leader-tab:<appId>:<dbName>`.
4. Candidate spawns the dedicated Jazz worker with the broker instance and leadership ID.
5. Worker acquires `jazz-leader-worker:<appId>:<dbName>`.
6. Worker opens the primary OPFS namespace and persistent runtime.
7. Leader tab reports `leader-ready` to the broker with the leadership ID and lock names
   it successfully holds.
8. Broker starts tab-lock and worker-lock monitors for the same leadership ID.
9. Broker announces the durable-ready leader and starts assigning follower
   ports.

The broker must not announce a leader as durable-ready before both locks are
held and the worker has opened the persistent runtime.

If promotion fails, the broker demotes that candidate for the failed leadership ID and
elects another connected tab. If no tab can be promoted, persistent browser mode
remains unavailable until a connected tab can satisfy the lock and worker
requirements.

Persistent browser `createDb` must not resolve for leader or follower tabs until
the durable path for that tab is ready. For followers, that means the broker has
a durable-ready leader, the follower data port has been transferred, and the
leader worker has acknowledged the follower attachment. Jazz must not expose a
Db instance that buffers durable writes while waiting for this state. Promotion
failures caused by unsupported APIs, configuration mismatch, or stale-worker
OPFS open failures reject with explicit errors.

## Demotion And Forced Takeover

Normal demotion:

1. Broker cancels lock monitors for that leadership ID and marks the transition as planned.
2. Broker sends `demote` to the old leader tab.
3. Leader tab marks the leadership ID stale and refuses new durable work.
4. Leader tab closes follower data ports for the leadership ID.
5. Leader tab asks the worker bridge to shut down cleanly.
6. Worker marks the leadership ID stale, rejects new peer attachments and durable work,
   then flushes and closes the persistent runtime.
7. Worker closes OPFS and releases the worker lock.
8. Leader tab terminates the dedicated worker.
9. Leader tab releases the tab lock.

The leader tab and leader worker must also enter this stale/poisoned state if
they observe any of these signals:

- broker port failure or broker instance change
- `demote` for their current leadership ID
- worker-side promotion failure
- explicit shutdown
- local detection that their leadership ID no longer matches current broker state

`forceTakeoverTimeoutMs` defaults to `1000` and must be configurable for tests
and browser-specific tuning.

If a demoted leader does not release the tab lock or worker lock within
`forceTakeoverTimeoutMs`, the broker may use Web Locks `{ steal: true }` for the
stuck lock. The broker must release any stolen lock immediately and continue
replacement election.

Lock stealing is exceptional recovery only. It does not kill old JavaScript. If
a stale worker continues running after a stolen worker lock and still holds an
OPFS access handle, the replacement worker may fail to open OPFS. That failure
must be reported as a stale-worker/open failure, not hidden by falling back to
OPFS retry as the coordination mechanism.

The broker cannot directly terminate a tab-owned dedicated worker. Worker
termination authority is mediated through the leader tab; the broker owns the
decision to demote and, after timeout, to steal locks.

Because lock stealing does not terminate JavaScript, it is only a recovery gate
for future promotion attempts. A stolen lock does not make the old leader safe.
Any old leader that later reconnects to the broker with a stale broker instance or leadership ID
must be rejected and must keep its local Db poisoned rather than resuming
durable work.

## Control Protocol

The protocol must stay small. Message names below describe the required
semantic boundary; exact TypeScript shapes can be adjusted during
implementation as long as the behavior stays equivalent.

Tab to broker:

- `hello`
  - identifies `tabId`, `appId`, `dbName`, and configuration fingerprint
- `visibility`
  - reports `visible` or `hidden`
- `leader-ready`
  - sent by a promoted leader after tab lock, worker lock, and persistent
    runtime are ready
- `leader-failed`
  - sent by a candidate or leader that cannot complete promotion or detects its
    own worker failure
- `follower-port-attached`
  - sent after the leader worker acknowledges a follower port
- `storage-reset-request`
  - asks the broker to coordinate reset
- `shutdown`
  - tells the broker this tab is intentionally closing its participation
- `broker-pong`
  - replies to broker liveness checks for the current broker instance

Broker to tab:

- `broker-hello`
  - announces the broker instance accepted for this port
- `broker-ping`
  - lightweight broker liveness check; this is not leader throttling detection
- `become-leader`
  - asks a candidate to acquire the tab lock and start the dedicated worker
- `demote`
  - asks a leader to shut down and release locks
- `leader-ready`
  - announces the durable-ready leader and leadership ID
- `attach-follower-port`
  - sends one endpoint of a `MessageChannel` to the leader tab so it can
    transfer that endpoint into the dedicated worker
- `use-follower-port`
  - sends the other endpoint of the same `MessageChannel` to the follower tab
- `follower-ready`
  - tells the follower its port has been accepted by the leader worker
- `close-follower-port`
  - tells a tab to close a stale follower data port for a dead leadership ID
- `storage-reset-begin`
  - freezes ordinary port assignment and starts reset coordination
- `storage-reset-finished`
  - reports reset success or failure
- `unsupported`
  - reports an unsupported environment or incompatible connection

Every broker-to-tab control message carries `brokerInstanceId`. Every leader-specific
control message carries `leadershipId`. Tabs must ignore stale broker instance/leadership ID messages,
close stale follower data ports, and reject stale leader-worker acknowledgements.

Leader tab to dedicated worker:

- existing init and direct leader `WorkerBridge` traffic remain
- `attach-follower-port`
  - transfers a follower `MessagePort` plus peer id and leadership ID
- `detach-follower-port`
  - closes a follower peer for a leadership ID

Leader dedicated worker to leader tab:

- `follower-port-attached`
  - confirms that the worker mapped the port to a peer client id
- `follower-port-closed`
  - reports peer closure or port failure
- existing worker bridge messages remain for the leader tab's own runtime

## Follower Data Ports

Follower data ports terminate inside the leader dedicated worker.

Data path:

```text
Follower main runtime
  -> MessagePort
  -> leader dedicated worker
  -> persistent runtime / OPFS / upstream sync
```

The broker creates a `MessageChannel` per follower peer attachment:

- one port is transferred to the follower tab,
- one port is transferred to the leader tab,
- the leader tab transfers its endpoint into the dedicated worker.

After transfer, the broker is not in the data path.

The data port carries binary Jazz sync payload batches. It must not carry
election messages, storage reset messages, or browser lifecycle messages. The
leadership ID and peer identity are established when the port is attached; a new leader
leadership ID gets new ports.

The leader worker maps each follower port to a runtime peer client. On port
close or `detach-follower-port`, the worker closes that peer client.

Follower tabs must not spawn a dedicated worker. Their main-thread runtime uses
the follower data port as its durable/upstream path.

Follower data ports are opened only after a three-way acknowledgement:

1. broker creates the `MessageChannel` for a follower and current leadership ID,
2. leader worker acknowledges the worker-side endpoint and peer id,
3. broker sends `follower-ready` to the follower.

The follower's persistent Db startup promise resolves only after step 3.

## Storage Reset

Storage reset moves into the broker protocol.

Reset flow:

1. Any tab sends `storage-reset-request`.
2. Broker freezes new follower port attachment.
3. Broker asks tabs to close existing follower data ports.
4. Broker demotes the current leader.
5. Leader tab shuts down and terminates the dedicated worker.
6. Broker waits for tab and worker locks to release, using the same forced
   takeover recovery if needed.
7. Broker elects a reset coordinator using normal visibility ranking.
8. Coordinator becomes leader and starts the worker in reset mode.
9. Worker deletes the primary OPFS namespace before opening a clean persistent
   runtime.
10. Leader reports `leader-ready`.
11. Broker reconnects followers with fresh ports.
12. Broker sends `storage-reset-finished`.

The reset path replaces the current BroadcastChannel storage reset coordinator.

## Shutdown

Follower tab shutdown:

- send `shutdown` to the broker,
- close the broker port,
- close any follower data port,
- shut down the main-thread runtime.

Leader tab shutdown:

- send `shutdown` to the broker,
- broker treats this as intentional demotion,
- close follower data ports for the leadership ID,
- gracefully shut down the worker bridge,
- terminate the dedicated worker,
- release locks,
- shut down main-thread runtime.

Unexpected broker-port close from the current leader, as observed by a running
broker, is treated as leader loss. The broker elects a replacement from
remaining connected tabs.

## Broker Failure And Restart

The SharedWorker broker is in-memory. If it crashes or is restarted, its
connected tabs must treat the new broker as a new coordination broker instance.

Because `MessagePort` close is not uniformly observable across browsers, the
broker sends a lightweight control-plane `broker-ping` for each active tab port.
Tabs reply with `broker-pong`. Missed broker pings, broker-port errors, or a
changed `brokerInstanceId` are fatal to the tab's current broker participation:

1. a leader tab marks its leadership ID stale, refuses new durable work, closes follower
   ports, shuts down its worker, and releases locks;
2. a follower closes its follower data port and marks persistent startup/runtime
   unavailable;
3. the tab reconnects by constructing the SharedWorker broker again and sending
   a fresh `hello`.

The new broker elects from scratch. It must not try to reconstruct the previous
leadership ID from tab claims because leadership IDs were broker-memory state. This ping/pong is
only broker liveness and broker instance detection; it must not be used to infer that a
leader tab or leader worker is throttled.

## Testing Strategy

Prefer black-box browser integration tests over white-box unit tests. Tests
should use the public `createDb` / framework APIs and real browser primitives
where the runner supports them.

Representative coverage:

- persistent browser mode fails fast with explicit errors when `SharedWorker`,
  `MessageChannel`, or Web Locks are missing
- first visible tab becomes leader and spawns the only dedicated persistent
  worker
- mismatched configuration fingerprints are rejected with explicit errors
- follower tab does not spawn a dedicated worker
- follower startup waits for leader-worker follower-port acknowledgement
- `createDb` does not resolve before the durable leader/follower path is ready
- follower reads and writes persist through the leader worker
- leader tab-lock release elects the most recently visible connected tab
- leader worker-lock release elects a replacement
- planned demotion/reset cancels leadership ID monitors and does not report unexpected
  lock loss
- stuck leader demotion uses Web Locks `steal` only after
  `forceTakeoverTimeoutMs`
- stale leaders refuse durable work after demotion, broker instance change, or
  broker liveness failure
- broker restart creates a new broker instance and forces tabs to reconnect/re-elect
- hidden-only election chooses the most recently visible connected tab
- storage reset requested from a follower succeeds and reconnects all tabs
- old BroadcastChannel election, sync, reset, and fallback namespace code paths
  are removed

Pure unit tests are appropriate for:

- candidate visibility ranking
- protocol message validation
- unsupported-environment error formatting

Do not rewrite existing behavior tests merely to match new implementation
details. If an existing test encodes behavior that conflicts with this spec,
surface the conflict during implementation.

## Migration Notes

- `createDb` and framework bindings keep their public shape.
- Persistent browser mode becomes stricter: unsupported environments throw an
  explicit initialization error instead of falling back to peer election.
- Brokered leaders do not hold the old `jazz-leader-lock:<appId>:<dbName>` lock;
  a tab contending with an old-version leader hangs in the OPFS open retry for up
  to ~3.4 minutes and then fails with a generic handle-conflict error; there is
  no explicit mixed-version detection.
- Memory mode remains unaffected and does not require SharedWorker or Web Locks.
- The follower fallback namespace model is removed because followers no longer
  open OPFS workers.
- Worker host code gains dynamic follower-port peer attachment.
- Browser `Db` coordination code changes from BroadcastChannel election/sync to
  SharedWorker broker control messages.

## Future Throttling Workflow

Throttling detection is intentionally not implemented in this MVP.

A later design may add leader-worker health samples, such as event-loop delay or
round-trip timing, so the broker can distinguish an unreachable leader from a
throttled leader. Any future throttling demotion must avoid confusing a worker
under heavy load with a worker that is genuinely throttled or dead.

Until that future workflow exists, throttling suspicion must not trigger broker
demotion. The MVP failover triggers are lock loss, worker failure, tab
disconnect, broker instance/liveness failure, explicit demotion, promotion failure,
and storage reset.
