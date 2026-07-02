# Leader Election: JS → Rust Port

## Goal

Move the browser leader-election logic from TypeScript to Rust, **without changing any
observable behavior**. The wire protocol, timings, election outcomes, failover sequences,
storage-reset orchestration, and all public TypeScript APIs stay exactly as they are today.
All existing TypeScript tests must keep passing **unmodified** — they are the behavioral
contract for this port.

This is a port, not a redesign. Where the current JS has quirks, the Rust port reproduces
the quirks (they are called out explicitly in [Behavioral invariants](#behavioral-invariants)).

## Non-goals

- No protocol changes. `BROKER_CONTROL_PROTOCOL_VERSION` stays `"jazz-browser-broker-v3"`
  because the message shapes do not change.
- No changes to `Db`, `createDb`, or any public API in `packages/jazz-tools`.
- No porting of the worker-bridge / runtime plumbing (`worker-bridge.ts`,
  `LeaderWorkerConnectionRole`, `FollowerPortConnectionRole`). Those wrap `MessagePort`s
  around the WASM runtime and are inherently JS glue.
- No performance work. Same timers, same backoffs, same message counts.

## Current state (what exists today)

All under `packages/jazz-tools/src/`:

| File                                                       | LOC   | Role                                                                                                                          |
| ---------------------------------------------------------- | ----- | ----------------------------------------------------------------------------------------------------------------------------- |
| `worker/jazz-broker-worker.ts`                             | ~1190 | SharedWorker broker: tab registry, **leader election**, liveness pings, follower-port assignment, storage-reset orchestration |
| `runtime/browser-broker-protocol.ts`                       | ~430  | Wire message types, `selectLeaderCandidate`, `isStaleLeadershipId`, fingerprint creation, capability detection                |
| `runtime/browser-broker-client.ts`                         | ~755  | Tab-side broker client: SharedWorker connection, role tracking, reconnect, reset waiters                                      |
| `runtime/leader-lock.ts`                                   | ~225  | Web Locks API wrappers (`tryAcquireWebLock`, `monitorWebLockRelease`, `stealAndReleaseWebLock`)                               |
| `runtime/connection-manager/browser-connection-manager.ts` | ~875  | Tab-side promotion/demotion orchestration, worker spawn, OPFS reset, page lifecycle                                           |
| `runtime/connection-manager/browser-broker-utils.ts`       | ~110  | Fingerprint/db-name resolution helpers                                                                                        |

Topology: every tab connects to one SharedWorker (the _broker_). The broker elects one tab
as _leader_; the leader spawns a dedicated worker that owns OPFS storage and upstream sync.
Followers get `MessageChannel` ports to the leader tab. The broker detects leader death via
Web Lock monitors and ping/pong liveness, and re-elects.

## Target architecture

**Sans-IO core.** The Rust code is a deterministic, synchronous state machine:
events in → commands out. It never touches timers, `MessagePort`s, Web Locks, random
numbers, or the clock. A thin JS shell owns all browser I/O and drives the core.

```text
             ┌─────────────────────── SharedWorker ───────────────────────┐
             │  JS shell (jazz-broker-worker.ts, rewritten as shell)      │
             │   - onconnect / port.onmessage                             │
             │   - setTimeout / clearTimeout                              │
             │   - navigator.locks (via leader-lock.ts, unchanged)        │
             │   - new MessageChannel() + port transfer                   │
             │   - Date.now(), crypto.randomUUID()                        │
             │            │ events (JSON-safe)      ▲ commands            │
             │            ▼                         │                     │
             │  ┌──────────────────────────────────────────────┐         │
             │  │  Rust BrokerCore (wasm)                       │         │
             │  │  election, liveness, reset orchestration,     │         │
             │  │  follower attachment tracking                 │         │
             │  └──────────────────────────────────────────────┘         │
             └────────────────────────────────────────────────────────────┘
```

Why sans-IO:

- **Behavior preservation is testable.** The core is deterministic; every existing JS test
  scenario can be replayed as an event script in Rust with asserted command sequences.
- **`MessagePort` cannot cross the WASM boundary.** Ports must stay in JS regardless; the
  core references them by id.
- **Native `cargo test` covers the logic.** No browser, no wasm-pack needed for the state
  machine itself (the jazz-wasm wasm-pack test target is currently broken anyway; do not
  rely on it).

### Crate layout

```text
crates/
  jazz-browser-broker/          # NEW — pure sans-IO core, no wasm-bindgen, no async
    src/
      lib.rs
      protocol.rs               # wire message enums (serde), select_leader_candidate,
                                #   is_stale_leadership_id, normalize_positive_timeout
      broker.rs                 # BrokerCore state machine (port of jazz-broker-worker.ts)
      tab_client.rs             # Phase 2: TabClientCore (port of browser-broker-client.ts)
      tests/                    # black-box event→command scenario tests
  jazz-broker-wasm/             # NEW — tiny wasm-bindgen wrapper around BrokerCore only.
                                #   Separate from jazz-wasm so the SharedWorker loads a
                                #   small module, not the multi-MB runtime binary.
                                #   NOT published to npm: `private: true`, devDependency of
                                #   jazz-tools. Its wasm bytes are embedded base64 into the
                                #   bundled broker worker at build time, so consumers never
                                #   install it and no extra release is needed.
```

`jazz-browser-broker` must not depend on `jazz-tools` (the Rust crate), tokio, or anything
async. It is `#![forbid(unsafe_code)]`, plain data structures, fully deterministic.

Phase 2's `TabClientCore` is compiled into the **existing** `jazz-wasm` binary (the tab
already loads it), not into `jazz-broker-wasm`.

New dependency: `indexmap` (see [Ordered-map semantics](#ordered-map-semantics) — this is
correctness-critical, not a style choice).

### Distribution constraint (critical)

`scripts/bundle-broker-worker.mjs` bundles the broker SharedWorker into **one
self-contained ESM file**, because bundlers (Next/Turbopack, Vite, webpack) never recognize
it as a worker entry and copy it verbatim — relative imports 404 at runtime. Today the
comment there says "the broker is pure, wasm-free coordination logic".

The port breaks that assumption, so the wasm bytes must be **embedded base64 in the broker
bundle** and instantiated from memory. Fetching a sibling `.wasm` URL would 404 in exactly
the environments the bundling script exists to fix. This is why `jazz-broker-wasm` must
stay tiny.

WASM instantiation is async; the tab's hello timeout is 5s. The shell must **queue inbound
port messages (and `onconnect` events) until the core is instantiated**, then replay them
in arrival order. Update the comment in `bundle-broker-worker.mjs` accordingly.

## Core API

This section is normative: the implementing agent should start from these types. Naming
maps 1:1 to the JS it replaces so reviewers can diff behavior side by side.

### Wire messages (`protocol.rs`)

Wire shapes are fixed. Serde must produce/consume exactly today's JSON: `type` tags are
kebab-case, fields are camelCase, optional fields are **omitted** when absent (not `null` —
see `reportLeaderReady` / `postStorageResetOutcome` in the JS, which spread-omit).

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility { Visible, Hidden }

/// Tab -> broker. Mirrors BrowserBrokerTabMessage.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TabMessage {
    #[serde(rename_all = "camelCase")]
    Hello {
        tab_id: String,
        app_id: String,
        db_name: String,
        fingerprint: String,
        visibility: Visibility,
        // Absent/invalid values fall back to defaults via normalization (see below).
        force_takeover_timeout_ms: Option<f64>,
        broker_ping_interval_ms: Option<f64>,
        broker_pong_timeout_ms: Option<f64>,
    },
    #[serde(rename_all = "camelCase")]
    Visibility { broker_instance_id: String, visibility: Visibility },
    #[serde(rename_all = "camelCase")]
    LeaderReady {
        broker_instance_id: String,
        leadership_id: u64,
        tab_lock_name: String,
        worker_lock_name: String,
        #[serde(default)]
        bridgeless_storage_reset: bool,
    },
    #[serde(rename_all = "camelCase")]
    LeaderFailed { broker_instance_id: String, leadership_id: u64, reason: String },
    #[serde(rename_all = "camelCase")]
    FollowerPortAttached { broker_instance_id: String, leadership_id: u64, follower_tab_id: String },
    #[serde(rename_all = "camelCase")]
    FollowerPortClosed { broker_instance_id: String, leadership_id: u64, follower_tab_id: String },
    #[serde(rename_all = "camelCase")]
    SchemaReady { broker_instance_id: String, schema_fingerprint: String },
    #[serde(rename_all = "camelCase")]
    StorageResetRequest { broker_instance_id: String, request_id: String },
    #[serde(rename_all = "camelCase")]
    StorageResetReady {
        broker_instance_id: String,
        request_id: String,
        success: bool,
        error_message: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Shutdown { broker_instance_id: String },
    #[serde(rename_all = "camelCase")]
    BrokerPong { broker_instance_id: String },
}
```

`ControlMessage` (broker → tab) mirrors `BrowserBrokerControlMessage` the same way, with
one exception: `attach-follower-port` and `use-follower-port` carry a `MessagePort`, so
they are **not** in the enum — they are emitted as the dedicated
`AttachFollowerChannel` command and serialized by the shell (see Commands).

Every `ControlMessage` includes `brokerInstanceId`. Unknown inbound fields are ignored
(serde default), unknown inbound `type`s are ignored (deserialize to an
`#[serde(other)]`-style catch-all and drop) — matching the JS `switch` falling through.

Pure helpers, ported byte-for-byte in behavior:

```rust
/// Port of selectLeaderCandidate (browser-broker-protocol.ts).
/// Prefer visible tabs; among the pool pick max last_visible_at;
/// tie-break: greater tab_id wins (byte comparison — tab ids are ASCII UUIDs,
/// so Rust byte order == JS UTF-16 code-unit order).
pub fn select_leader_candidate<'a, I>(candidates: I) -> Option<&'a Candidate>
where I: IntoIterator<Item = &'a Candidate>
{
    let all: Vec<&Candidate> = candidates.into_iter().collect();
    let visible: Vec<&Candidate> =
        all.iter().copied().filter(|c| c.visibility == Visibility::Visible).collect();
    let pool = if visible.is_empty() { &all } else { &visible };

    let mut selected: Option<&Candidate> = None;
    for candidate in pool {
        let Some(current) = selected else { selected = Some(candidate); continue };
        if candidate.last_visible_at > current.last_visible_at
            || (candidate.last_visible_at == current.last_visible_at
                && candidate.tab_id > current.tab_id)
        {
            selected = Some(candidate);
        }
    }
    selected
}

/// Port of isStaleLeadershipId: stale means strictly less than current.
pub fn is_stale_leadership_id(incoming: u64, current: u64) -> bool { incoming < current }

/// Port of normalizePositiveTimeout: non-finite / non-positive -> fallback,
/// else max(1, floor(v)).
pub fn normalize_positive_timeout(value: Option<f64>, fallback: u64) -> u64;

/// Port of normalizeForceTakeoverTimeout: invalid/negative -> default (1000),
/// else max(0, floor(v)). Note: unlike normalize_positive_timeout, zero is allowed.
pub fn normalize_force_takeover_timeout(value: Option<f64>) -> u64;
```

`createBrowserBrokerFingerprint`, `createRuntimeSourceIdentity`, and
`stableStringify` **stay in TypeScript**. They run tab-side, involve `WeakMap` object
identity and `BufferSource` hashing over JS objects, and the broker only ever
string-compares fingerprints. Porting them buys nothing and risks byte drift.

### Ids and effect handles

```rust
/// Shell-assigned identity of a connected MessagePort (monotonic counter in the shell).
/// The broker distinguishes "same tab id, different port" (hello re-connect), so tab ids
/// alone are not enough.
pub struct PortId(pub u64);

/// Handles for in-flight lock effects; allocated by the core, echoed back in events.
pub struct ProbeId(pub u64);
pub struct MonitorId(pub u64);
```

### Events (shell → core)

```rust
pub enum BrokerEvent {
    /// A tab message arrived on a port. The shell parses JSON via serde-wasm-bindgen;
    /// unparseable messages are dropped (matching the JS "not an object" guard).
    PortMessage { port_id: PortId, message: TabMessage },
    TimerFired { timer: TimerKey },
    /// Result of ProbeLocks: were ALL requested locks acquired-and-released?
    LocksProbeResult { probe_id: ProbeId, all_acquired: bool },
    /// StealLocks finished (individual steal errors are swallowed, as in JS).
    LocksStolen { probe_id: ProbeId },
    /// A monitored lock was granted to the broker OR the monitor errored.
    /// JS routes onGranted and onError to the same handler; keep one event.
    LockMonitorTriggered { monitor_id: MonitorId },
}
```

Every entry point takes the clock explicitly:

```rust
impl BrokerCore {
    pub fn new(broker_instance_id: String) -> Self;
    pub fn handle(&mut self, event: BrokerEvent, now_ms: i64) -> Vec<BrokerCommand>;
}
```

`Date.now()` must never be read inside the core. `now_ms` is `Date.now()` at the moment
the shell dequeues the event.

### Commands (core → shell)

```rust
pub enum BrokerCommand {
    /// port.postMessage(message) — message already carries brokerInstanceId.
    Post { port_id: PortId, message: ControlMessage },
    ClosePort { port_id: PortId },
    /// The assignFollowerPorts pair: shell creates one MessageChannel and posts
    ///   {type:"attach-follower-port", followerTabId, leadershipId, port: ch.port1}
    ///     to leader_port_id (transferring port1), then
    ///   {type:"use-follower-port", leaderTabId, leadershipId, port: ch.port2}
    ///     to follower_port_id (transferring port2) — in that order, synchronously.
    AttachFollowerChannel {
        leader_port_id: PortId,
        follower_port_id: PortId,
        leader_tab_id: String,
        follower_tab_id: String,
        leadership_id: u64,
    },
    SetTimer { timer: TimerKey, delay_ms: u64 },
    ClearTimer { timer: TimerKey },
    /// tryAcquireWebLock on every name concurrently, release all acquired leases,
    /// report all_acquired = every lease was obtained. (acquireAndReleaseLocks)
    ProbeLocks { probe_id: ProbeId, lock_names: Vec<String> },
    /// stealAndReleaseWebLock on each name sequentially, errors swallowed per lock.
    StealLocks { probe_id: ProbeId, lock_names: Vec<String> },
    /// monitorWebLockRelease(lock_name); shell keeps monitor handle keyed by monitor_id.
    MonitorLock { monitor_id: MonitorId, lock_name: String },
    CancelLockMonitor { monitor_id: MonitorId },
    /// console.warn for the once-only stale-instance drop diagnostic.
    WarnStaleInstanceDrop { message_type: String, tab_id: String, stamped_instance_id: String },
}
```

```rust
pub enum TimerKey {
    BrokerPing,
    FollowerAttachment { leadership_id: u64, follower_tab_id: String },
    LeaderFailureRetry,
    /// The sleep(forceTakeoverTimeoutMs) inside waitForPreviousLeaderLocks.
    ForceTakeoverSleep { probe_id: ProbeId },
}
```

The shell keeps a `Map<serializedTimerKey, timeoutHandle>`; `SetTimer` on a live key
replaces it (clearTimeout + setTimeout), matching JS reassignment.

### Core state (`broker.rs`)

Mirror the module-level globals of `jazz-broker-worker.ts` as struct fields — same names,
snake_cased. Do not "clean up" the state shape; the redundancy in the JS (e.g. three
parallel attachment structures) encodes behavior:

```rust
pub struct BrokerCore {
    broker_instance_id: String,
    tabs: IndexMap<String, TabState>,              // insertion-ordered! see below
    namespace: Option<Namespace>,
    leader: Option<LeaderState>,
    current_leadership_id: u64,
    pending_follower_attachments: IndexSet<AttachmentKey>,
    // Timers themselves live in the shell; the core tracks which keys are pending
    // only implicitly via pending_follower_attachments (as JS does via the timer map).
    follower_attachment_retry_counts: HashMap<AttachmentKey, u32>,
    attached_follower_ports: IndexSet<AttachmentKey>,
    warned_stale_instance_drop: bool,              // NOT reset by reset_if_idle (JS quirk)
    replacement_election_in_flight: bool,
    replacement_election_generation: u64,
    broker_ping_timer_running: bool,
    leader_failure_retry_timer_running: bool,
    reset_state: Option<ResetState>,
    completed_storage_reset_outcomes: IndexMap<String, StorageResetOutcome>, // ordered!
    failed_leader_retry_after_by_tab_id: HashMap<String, i64>,
    pending_takeover: Option<PendingTakeover>,     // replaces JS async continuations
    next_probe_id: u64,
    next_monitor_id: u64,
}

struct TabState {
    tab_id: String,
    app_id: String,
    db_name: String,
    fingerprint: String,
    schema_fingerprint: Option<String>,
    visibility: Visibility,
    last_visible_at: i64,
    port_id: PortId,        // JS holds the MessagePort; the core holds its id
    last_pong_at: i64,
}

struct LeaderState {
    tab_id: String,
    leadership_id: u64,
    ready: bool,
    tab_lock_name: Option<String>,
    worker_lock_name: Option<String>,
    tab_lock_monitor: Option<MonitorId>,
    worker_lock_monitor: Option<MonitorId>,
}

struct ResetState {
    request_id: String,
    request_ids: IndexSet<String>,
    participants: IndexSet<String>,
    prepared_tabs: IndexSet<String>,
    errors: Vec<String>,
    previous_leader: Option<ClearedLeaderState>,
    promoted_leadership_id: Option<u64>,
    phase: ResetPhase,      // Preparing | Promoting | Reconnecting
}

/// (leadershipId, followerTabId) — replaces the JS string key
/// `${leadershipId}:${followerTabId}`. clearFollowerAttachmentState's
/// endsWith(`:${tabId}`) scan becomes an equality filter on follower_tab_id;
/// equivalent because tab ids never contain ':'.
#[derive(Clone, PartialEq, Eq, Hash)]
struct AttachmentKey { leadership_id: u64, follower_tab_id: String }
```

### Async control flow → explicit state

The JS has three `async` flows that must become explicit state, resumed by events. This is
the only structural transformation in the port; everything else is line-by-line.

**1. `waitForPreviousLeaderLocks` (takeover probe).** JS: probe both previous-leader locks;
if not all acquired, `sleep(forceTakeoverTimeoutMs)`, re-check guard, steal both locks;
then continue to election / reset promotion. Port as:

```rust
struct PendingTakeover { probe_id: ProbeId, purpose: TakeoverPurpose }

enum TakeoverPurpose {
    /// scheduleReplacementElection: on completion clear in_flight (if generation
    /// matches) and run elect_if_needed.
    ReplacementElection { generation: u64 },
    /// continueStorageResetIfReady's promoting phase: on completion check errors,
    /// then finish_storage_reset or promote_reset_leader.
    StorageReset { request_id: String },
}
```

Flow: emit `ProbeLocks` → on `LocksProbeResult{all_acquired: true}` continue → else
`SetTimer(ForceTakeoverSleep)` → on fire, re-evaluate the guard (`shouldForceTakeover` in
JS: for ReplacementElection that is `in_flight && generation matches && leader.is_none()`;
for StorageReset that is `reset_state` still being the same request) → `StealLocks` → on
`LocksStolen` continue. If the previous leader had no lock names, skip straight to the
continuation (JS early-returns). Guards are re-checked at **every** resumption point,
exactly where the JS `await`s return.

**2. `scheduleReplacementElection` generation guard.** `replacement_election_in_flight`
and `replacement_election_generation` port directly; the `finally`-block semantics (clear
`in_flight` only if the generation still matches) must be preserved at every exit path of
the takeover flow.

**3. Storage-reset phases.** `preparing → promoting → reconnecting` port as `ResetPhase`.
`continueStorageResetIfReady` re-runs on: every `storage-reset-ready`, every participant
removal, and reset-start. It advances to `Promoting` only when all participants are
prepared, then enters the takeover flow above.

### wasm-bindgen surface (`jazz-broker-wasm`)

Keep it minimal and dumb:

```rust
#[wasm_bindgen]
pub struct WasmBrokerCore { inner: BrokerCore }

#[wasm_bindgen]
impl WasmBrokerCore {
    #[wasm_bindgen(constructor)]
    pub fn new(broker_instance_id: String) -> WasmBrokerCore;

    /// event: a JS object (serde-wasm-bindgen); returns an array of command objects.
    /// now_ms: Date.now().
    pub fn handle(&mut self, event: JsValue, now_ms: f64) -> Result<JsValue, JsError>;
}
```

Commands cross the boundary as plain JSON-safe objects with a `kind` tag; the shell
`switch`es on `kind`. No callbacks into JS from Rust, no `js_sys` in the core.

## Behavioral invariants

The implementing agent must preserve every one of these. Each is anchored to the current
source; write a Rust test for each before porting the corresponding logic
(`crates/jazz-browser-broker/src/tests/`).

### Constants (values must not change)

| Constant                                 | Value                        | Source                     |
| ---------------------------------------- | ---------------------------- | -------------------------- |
| `DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS`      | 1 000                        | jazz-broker-worker.ts      |
| `LEADER_FAILURE_RETRY_BACKOFF_MS`        | 1 000                        | jazz-broker-worker.ts      |
| `INITIAL_FOLLOWER_ATTACHMENT_TIMEOUT_MS` | 1 000                        | jazz-broker-worker.ts      |
| `MAX_FOLLOWER_ATTACHMENT_TIMEOUT_MS`     | 30 000                       | jazz-broker-worker.ts      |
| `COMPLETED_STORAGE_RESET_OUTCOME_TTL_MS` | 30 000                       | jazz-broker-worker.ts      |
| `MAX_COMPLETED_STORAGE_RESET_OUTCOMES`   | 100                          | jazz-broker-worker.ts      |
| `DEFAULT_BROKER_PING_INTERVAL_MS`        | 1 000                        | browser-broker-protocol.ts |
| `DEFAULT_BROKER_PONG_TIMEOUT_MS`         | 3 000                        | browser-broker-protocol.ts |
| Follower attachment backoff              | `min(1000 · 2^retry, 30000)` | `markFollowerPortPending`  |

### Election

1. **Candidate selection** (`selectLeaderCandidate`): visible tabs preferred; if none
   visible, all tabs are the pool. Max `lastVisibleAt` wins; ties broken by greater
   `tabId` (string byte comparison).
2. **Eligibility** (`eligibleLeaderCandidates`): a tab is excluded if it is in leader
   failure backoff, or if a canonical namespace `schemaFingerprint` exists and the tab's
   fingerprint differs. Backoff entries for departed tabs self-clean on read.
3. **`electIfNeeded` guards, in order**: skip if a reset is active; skip if a replacement
   election is in flight; skip if leader is already ready or there are no tabs; skip if a
   (not-ready) leader exists and the namespace has **no** schema fingerprint yet. If a
   not-ready leader exists whose tab holds the canonical fingerprint, keep it. If the
   selected candidate _is_ the current not-ready leader, keep it. Otherwise clear the old
   leader (with demote) and promote the candidate.
4. **`leadershipId` is monotonic per broker instance**, incremented on every promotion
   (`currentLeadershipId += 1`), including reset promotions. Stale = strictly less.
5. **No candidate available** → `scheduleLeaderFailureRetryElection`: one timer armed at
   the earliest retry-after among still-connected failed tabs; on fire, `electIfNeeded`.
   The timer is not armed if a reset/replacement election is pending or leader is ready.
6. **`leader-ready` from a non-leader or wrong leadership** → reply `demote` with the
   message's leadershipId to that tab only.
7. **Lock monitors**: started only when `leader-ready` supplies both lock names; both
   monitors route to the same handler; a trigger for the _current_ leadershipId clears the
   leader **and removes the leader tab** (`removeLeaderTab: true` — this is the crashed-tab
   path), then schedules a replacement election.

### Liveness (quirk — preserve exactly)

8. `startBrokerPingTimer` fires `sendBrokerPings()` immediately when called with no timer
   running, then arms the interval. On expiry the callback pings once, and — if tabs
   remain — calls `startBrokerPingTimer` again, which pings **again** immediately and
   re-arms. Net effect: two back-to-back ping sweeps per interval boundary. Reproduce
   this; do not fix it.
9. Pong timeout is strict: evict when `now - lastPongAt > brokerPongTimeoutMs`. Eviction
   sweeps run at ping time and on every `visibility` message, iterating a **snapshot** of
   the tab list.
10. Evicting the leader tab follows the same clear/replace path as `shutdown` of a leader,
    including the mid-reset repromotion branch (see 20).

### Hello / namespace

11. The **first** hello latches the namespace: appId, dbName, fingerprint, and the three
    normalized timeouts. Later hellos must match appId+dbName+fingerprint exactly or get
    `unsupported` (code `INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE`) and their port
    closed, without touching state.
12. A hello for an existing tabId on a **different** port closes the old port and replaces
    the entry; attachment state for that tab is cleared first.
13. Post-hello sequence: `broker-hello`, then redelivery of **all** remembered
    storage-reset outcomes (pruned first). If **any** reset is active, the handler stops
    there (`addTabToActiveReset` only joins the tab — participant +
    `storage-reset-begin` with the pre-reset leadershipId — when the phase is
    `preparing`; in later phases the tab gets nothing until the reset finishes).
    Otherwise: if the arriving tab _is_ the recorded leader, clear that leader (no
    demote, keep tab); then if a ready leader exists, send `leader-ready` and run
    follower-port assignment; else `electIfNeeded`.
14. **Instance stamping**: every non-hello message whose `brokerInstanceId` differs from
    the current instance is dropped; the first such drop emits the console warning
    (`warnedStaleInstanceDrop` — never reset, even by `resetIfIdle`).
15. `resetIfIdle` (last tab gone): clear namespace, leader, all attachment state, reset
    state, bump replacement generation, clear in-flight flag, clear failure backoffs, stop
    ping + retry timers. Completed reset outcomes are **kept** (they are TTL-pruned).

### Follower ports

16. `assignFollowerPorts` is suppressed while a reset is active unless phase is
    `reconnecting`. A follower is assigned iff it is not the leader tab and (no canonical
    fingerprint || fingerprints match), and the (leadership, follower) key is neither
    pending nor attached.
17. Attachment timeout: on expiry, if the key was still pending and the same leadership is
    still ready and the follower still exists, bump the retry count and re-run assignment.
    Retry counts are cleared when the pending entry is cleared via
    `clearPendingFollowerAttachment` (attached, detached, tab removed) but survive the
    timeout path itself.
18. `follower-port-attached` (from the current leader, current leadership, key pending):
    clear pending, mark attached, send `follower-ready` to the follower, and if a reset is
    reconnecting on this leadership, try to finish it.
19. Any leader clear (`clearLeader`) sends `close-follower-port` to every other tab and
    wipes **all** attachment state.

### Storage reset

20. Phases: `preparing` (all participants must report `storage-reset-ready`) →
    `promoting` (takeover-probe the previous leader's locks; if any participant reported
    an error, fail the whole reset) → promote a candidate with `become-leader` carrying
    `resetRequestId` → new leader wipes OPFS then reports `leader-ready` →
    `reconnecting` (all eligible participants must re-attach follower ports) → finish.
    If the promoted leader fails/shuts down/is evicted mid-reset (phase ≠ preparing),
    record the error (leader-failed only), clear `promotedLeadershipId`, and re-promote.
21. Duplicate reset requests while one is active: the requestId joins `requestIds`, the
    requester gets `storage-reset-started`, and all accumulated requestIds are settled
    together at finish.
22. **Bridgeless fresh-namespace path**: `leader-ready` with `bridgelessStorageReset` set,
    matching `promotedLeadershipId`, and the leader tab having **no** schemaFingerprint →
    clear leader (with demote, keep tab) and finish the reset successfully. Do not mark
    the leader ready. (This is the reset-hang fix; regression-tested in the TS suite.)
23. Finished outcomes are remembered with delete-before-insert so a re-finished requestId
    moves to the back of the eviction order; TTL 30s, cap 100, oldest-first eviction.
    Outcomes are broadcast to all tabs at finish and redelivered on every hello.
24. A failed reset finish triggers `electIfNeeded`.
25. Tabs joining mid-reset (phase `preparing` only) become participants and get
    `storage-reset-begin` with the **previous** leader's leadershipId (or current).
    Participant removal at any phase re-runs `continueStorageResetIfReady`.

### Schema fingerprints

26. `schema-ready`: the first reported fingerprint becomes the namespace's canonical one.
    A tab reporting a **different** fingerprint gets `schema-blocked` but stays connected
    (it keeps answering pings and can be adopted later); if that tab is the current
    leader, clear the leader (with demote) and schedule a replacement election. A
    matching report triggers `assignFollowerPorts` if a leader is ready, else
    `electIfNeeded`.
27. `reelectSchemaFingerprintIfUnheld` (on every tab removal): if the departed tab held
    the canonical fingerprint and no remaining tab does, adopt the fingerprint of the
    **first-inserted** remaining tab that has one (or clear it). If a new fingerprint was
    adopted: `assignFollowerPorts` when a leader is ready, else `electIfNeeded`. This is
    what lets schema-blocked tabs recover without a reload.

### Ordered-map semantics

The port **must** use insertion-ordered maps (`IndexMap`/`IndexSet` with `shift_remove`,
never `swap_remove`) wherever JS iterates a `Map`/`Set`:

- `tabs`: `reelectSchemaFingerprintIfUnheld` adopts the fingerprint of the
  **first-inserted** remaining tab that has one. `announceLeaderReady`,
  `assignFollowerPorts`, ping sweeps, and reset broadcasts iterate in insertion order
  (message ordering per port is observable in tests).
- `completedStorageResetOutcomes`: eviction order is insertion order; see invariant 23.
- `resetState.participants` / `preparedTabs` / `requestIds`: iteration order affects
  outcome settlement order.

### Numbers and time

- `leadershipId`, generations, retry counts: `u64`/`u32` in Rust; they are JS numbers on
  the wire (serde handles this — values stay far below 2^53).
- All timestamps are `i64` milliseconds fed from `Date.now()` by the shell.
- The core allocates `ProbeId`/`MonitorId` from monotonic counters; ids never recycle
  within an instance.

## JS shell (rewritten `jazz-broker-worker.ts`)

The shell should end up under ~200 lines:

1. Instantiate wasm from embedded bytes; buffer `onconnect`/`message` events until ready,
   then replay in order.
2. Generate `brokerInstanceId` via `createRandomId("broker")` (stays TS).
3. Maintain `Map<PortId, MessagePort>` (assign ids on `onconnect`) and
   `Map<string, timeoutHandle>` for timers.
4. For each port message: parse, call `core.handle({...}, Date.now())`, execute returned
   commands in order, synchronously. Command execution must not re-enter the core except
   via new events.
5. Implement lock commands with the **unchanged** `leader-lock.ts` helpers.
6. `AttachFollowerChannel`: `new MessageChannel()`, post both messages with transfers, in
   the order specified on the command doc.

`leader-lock.ts`, `browser-broker-errors.ts`, and the fingerprint helpers stay as they are.

## Phasing

**Phase 1 — broker core (this spec's main scope).** Port `jazz-broker-worker.ts` +
the pure helpers of `browser-broker-protocol.ts` into `jazz-browser-broker` +
`jazz-broker-wasm`; rewrite the SharedWorker as the shell. TS message _type definitions_
in `browser-broker-protocol.ts` remain the source of truth for the TS side; add a
round-trip test asserting Rust serde output matches representative TS fixtures.

**Phase 2 — tab client core.** Port the `BrowserBrokerClient` decision logic into
`tab_client.rs`. Normative design below (see "Phase 2: tab client core API").

**Phase 3 (optional, separate effort) — connection-manager promotion state machine.**
`BrowserConnectionManager`'s promotion/demotion/cancellation logic could follow the same
pattern, but it is entangled with worker spawning, OPFS deletion, and page lifecycle. If
attempted: the durable-path waiter semantics are load-bearing (non-terminal port closures
must _not_ reject waiters — regression history) and every `finishCancelledBrokerPromotion`
checkpoint must survive verbatim. Not required for this project's definition of done.

## Phase 2: tab client core API

Port of `browser-broker-client.ts` decision logic into `tab_client.rs`
(`jazz-browser-broker` crate), exposed as `WasmTabBrokerCore` from the **existing
`jazz-wasm` binary** (new `broker_client` module). No new distribution artifact: the tab
already loads jazz-wasm, and `createDbWithRuntimeModule` awaits `runtimeModule.load()`
**before** `connectionManager.start()`, so the binary is always initialized before the
broker client connects. For standalone use (unit tests in Node), the shell calls
`loadWasmModule(options.runtimeSources)` itself — it is idempotent and has a packaged-
bytes Node bootstrap path.

### Boundary rules (timing contract — do not violate)

`browser-broker-client.test.ts` dispatches fake-port messages **synchronously after the
`connect()` call** and uses fake timers around the hello timeout. Therefore:

1. **The synchronous prefix of `connectToBroker` stays in the shell, unchanged**:
   `createSharedWorker()`, listener attachment, `port.start()`, hello-promise setup with
   its 5s `setTimeout`, worker `error` listener, and the hello `postMessage` all happen
   synchronously. (Hello needs no core: it is the one unstamped message.)
2. Core init (`await loadWasmModule(...)`, `new WasmTabBrokerCore(...)`) happens _after_
   that prefix. Control messages arriving earlier are **queued by the shell and replayed
   in order** once the core exists — before `connect()` resolves.
3. `closedError` stays a JS `Error` **instance** owned by the shell (tests assert
   `instanceof IncompatibleBrowserBrokerConfigurationError` and `error.cause` identity).
   The core signals `CloseWithError { message, code }`; the shell constructs/stores the
   typed error and passes it to waiter rejections and `onClosed`.
4. Async **choreography** stays in the shell where the JS awaits callbacks: the reconnect
   sequence and the `onStorageResetBegin` promise chain. The core makes every decision
   and emits the steps; the shell executes them in order.
5. Promise plumbing (`waitForRole`, reset waiters) stays in the shell keyed by shell-
   allocated waiter ids; the core decides settlement.

### Core types

```rust
pub struct TabClientCore { /* mirrors BrowserBrokerClient fields */ }

impl TabClientCore {
    /// tab_id + normalized liveness inputs + storage-reset start timeout.
    pub fn new(options: TabClientOptions) -> Self;
    pub fn handle(&mut self, event: TabClientEvent) -> Vec<TabClientCommand>;
    pub fn snapshot(&self) -> TabClientSnapshot; // brokerInstanceId, role, leaderTabId, leadershipId, closed, reconnecting
}

pub enum TabTimerKey {
    Liveness,
    RoleWaiter { waiter_id: u64 },
    ResetStartWaiter { waiter_id: u64 },
}

pub enum TabClientEvent {
    /// A (re)connected port finished the hello handshake path far enough to
    /// process control traffic. Fired by the shell right after core init or
    /// after a reconnect's connectToBroker succeeds.
    PortAttached,
    /// Inbound control message with any MessagePort stripped by the shell
    /// (the shell holds the port for the matching Invoke* command).
    ControlMessage { message: TabControlMessage },
    PortMessageError,
    TimerFired { timer: TabTimerKey },
    /// connect() finished waitForInitialLeadershipMessage → arm liveness.
    ConnectCompleted,
    RoleWaiterAdded { waiter_id: u64, role: Role, timeout_ms: u64 },
    StorageResetRequested { request_id: String, start_waiter_id: u64, completion_waiter_id: u64 },
    /// report*()/send() surface: unstamped tab message from the public API.
    SendRequested { message: UnstampedTabMessage },
    VisibilityReported { visibility: Visibility },
    ShutdownRequested,
    /// Shell finished the reconnect choreography (None = success).
    ReconnectFinished { error: Option<String> },
}

pub enum TabClientCommand {
    PostToBroker { message: TabMessage },          // stamped, ready to post
    SetTimer { timer: TabTimerKey, delay_ms: u64 },
    ClearTimer { timer: TabTimerKey },
    SettleRoleWaiter { waiter_id: u64, error: Option<String> },
    SettleResetStartWaiters { waiter_ids: Vec<u64>, error: Option<ResetWaiterError> },
    SettleResetWaiters { waiter_ids: Vec<u64>, error: Option<ResetWaiterError> },
    InvokeOnBecomeLeader { leadership_id: u64, reset_request_id: Option<String> },
    InvokeOnDemote { leadership_id: u64 },
    InvokeOnAttachFollowerPort { follower_tab_id: String, leadership_id: u64 },
    InvokeOnUseFollowerPort { leadership_id: u64 },
    InvokeOnFollowerReady { leadership_id: u64 },
    InvokeOnCloseFollowerPort { leadership_id: u64 },
    InvokeOnDetachFollowerPort { follower_tab_id: String, leadership_id: u64 },
    InvokeOnStorageResetBegin { request_id: String, leadership_id: u64 },
    InvokeOnSchemaBlocked { reason: String },
    InvokeOnReconnected,
    /// onBrokerPing + optional pong. The pong decision (respondToBrokerPings
    /// may be a function) is evaluated by the shell; the pong is stamped with
    /// the **ping's** instance id, which this command carries.
    HandleBrokerPing { broker_instance_id: String },
    /// Shell: detach+close current port. Emitted on close/reconnect/shutdown.
    DetachPort,
    /// Shell: run the JS reconnect choreography (await onDemote if
    /// previous_leadership_id > 0, onCloseFollowerPort if previous follower,
    /// connectToBroker, then feed ReconnectFinished).
    StartReconnect { previous_role: Role, previous_leadership_id: u64 },
    /// Shell: build typed Error (code → IncompatibleBrowserBrokerConfigurationError),
    /// store as closedError, reject remaining waiters, invoke onClosed.
    CloseWithError { message: String, code: Option<String> },
    /// ResetWaiterError::Closed → reject with the stored closedError instance;
    /// ::Message(s) → reject with new Error(s).
    ...
}
```

`TabControlMessage` extends the Phase 1 `ControlMessage` enum with the two port-carrying
variants (`attach-follower-port`, `use-follower-port`) minus their `port` field; the
shell strips the port before deserialization and pairs it back on the Invoke command
(events and commands are processed strictly in order, one event at a time, so "the port
of the message currently being handled" is unambiguous).

### Tab-client invariants

- T1. Stamping: hello passes through untouched; every other outbound message gets the
  current `brokerInstanceId`; a null instance id drops the message silently.
- T2. `send()` rules, in order: closed → drop; stamp fails → drop; reconnecting → drop;
  no port → queue; else post. The queue is flushed in order after connect and after a
  successful reconnect — and **cleared without replay** when a reconnect starts.
- T3. `broker-hello` records the instance id. The mismatch guard runs **before** the
  message dispatch: any control message stamped with a different instance id (while one
  is set) triggers a reconnect and is not otherwise processed.
- T4. `become-leader`: adopt leadershipId, invoke `onBecomeLeader`; a rejected callback
  reports `leader-failed` with the stringified error (shell catches the promise).
- T5. `demote`: matching leadershipId → role=follower, leaderTabId=null, re-resolve role
  waiters. `onDemote` is invoked **regardless** of the id match (future demotes cancel
  in-flight promotions).
- T6. `leader-ready`: adopt id+leader; role = (leaderTabId == tabId); resolve role
  waiters (a waiter resolves only when role matches **and** leaderTabId is non-null).
- T7. `attach-follower-port` is ignored unless leadershipId matches exactly.
  `use-follower-port` / `follower-ready` adopt the id and force role=follower.
- T8. Storage reset: `storage-reset-begin`/`-started`/`-finished` all resolve start
  waiters; `-begin` additionally runs `onStorageResetBegin` and replies
  `storage-reset-ready` (success / stringified error); `-finished` settles completion
  waiters (reject `errorMessage ?? "Browser storage reset failed"` on failure). Only the
  **start** acknowledgment has a timeout (`storageResetTimeoutMs`, default 5000).
- T9. `requestStorageReset` throws the stored closedError when closed, and loops
  `await reconnectDone` while a reconnect is in flight before sending.
- T10. Liveness: timeout = normalized pingInterval (default 1000) + normalized
  pongTimeout (default 3000); re-armed on every `broker-ping` and once after connect;
  expiry triggers a reconnect. Pong reply is optional (`respondToBrokerPings`, default
  true) and stamped with the ping's instance id.
- T11. Reconnect sequence: guard (closed/reconnecting) → set reconnecting → capture
  previous role/leadership → stop liveness → reset instance/role/leader/leadership →
  clear queue → reject reset+start waiters ("Browser broker restarted during storage
  reset") → detach old port → await `onDemote` if previous leadership > 0 →
  `onCloseFollowerPort` if previously follower with leadership > 0 → reconnect →
  on failure `closeWithError` (preserving the thrown error as `cause`) → on success
  send current visibility, invoke `onReconnected`, flush queue.
- T12. `unsupported` → `closeWithError` with the typed error; `onClosed` invoked exactly
  once; all waiters rejected; port detached; later sends drop.
- T13. `shutdown()`: idempotent; stamps and posts the shutdown message **before** marking
  closed (skipped when no instance id yet); rejects all waiters with "Browser broker
  client closed".
- T14. `waitForRole`: immediate resolve when role already matches with a non-null
  leaderTabId; immediate throw of closedError when closed; timeout rejects
  "Timed out waiting for broker role {role}".
- T15. The 100ms initial-leadership quiet window after hello (skipped when a leadership
  message already arrived) runs before `connect()` resolves.
- T16. Constants: hello timeout 5000 (registered synchronously — fake-timers contract),
  quiet window 100, `DEFAULT_STORAGE_RESET_TIMEOUT_MS` 5000, `waitForRole` default 5000.

## Testing

- **Rust:** black-box scenario tests only — construct `BrokerCore`, feed event sequences,
  assert full command sequences. No reaching into private state. Re-express each scenario
  from `broker-worker-bundle.test.ts`, `browser-broker-protocol.test.ts`, and the broker
  cases in `browser-broker-client.test.ts` as an event script. Read
  `crates/jazz-tools/TESTING_GUIDELINES.md` before writing tests; its black-box rule
  applies (the event/command API _is_ this crate's public API — the DB-topology helpers it
  mentions don't apply to this crate).
- **TypeScript:** all existing suites pass **unchanged**. If a TS test fails, the port is
  wrong — never adjust the test (per repo policy, that is a human-in-the-loop decision).
- **Fixtures:** golden JSON fixtures for every wire message variant, generated from the
  TS types, round-tripped through Rust serde (field names, tag values, optional-field
  omission).
- **E2E:** the existing browser E2E flows (multi-tab failover, storage reset) run against
  the wasm broker as the final gate. `pnpm build:core`, then the browser test configs in
  `packages/jazz-tools`.

## Code style for the Rust core

- No `unsafe`, no `unwrap`/`expect` in non-test code. Invalid input is _ignored_, not an
  error — mirroring the JS guard-and-return style. `handle` never fails; the wasm wrapper
  only surfaces serde errors for genuinely malformed event objects.
- No logging inside the core; diagnostics are commands (see `WarnStaleInstanceDrop`).
- Keep JS names: `elect_if_needed`, `clear_leader`, `assign_follower_ports`,
  `continue_storage_reset_if_ready`, etc. A reviewer must be able to read the Rust
  side-by-side with the deleted TS.
- Comments: port the JS comments that explain _why_ (e.g. the delete-before-set Map
  ordering note, the bridgeless-reset rationale, the schema-fingerprint re-election
  comment). Do not add narration comments.
- Each handler is a method on `BrokerCore` pushing into a `Vec<BrokerCommand>` (either
  passed down as `&mut` or a small `Effects` accumulator) — pick one and use it uniformly.

## Definition of done

1. `jazz-broker-worker.ts` contains only the shell; all decision logic lives in
   `crates/jazz-browser-broker`.
2. Every invariant above has a Rust test.
3. All existing TS tests and E2E suites pass without modification.
4. The broker bundle remains a single self-contained file; `pnpm build:core` produces it.
5. `cargo test -p jazz-browser-broker` runs natively (no wasm toolchain required).

## Open questions

- Final crate names (`jazz-browser-broker` / `jazz-broker-wasm`) — bikeshed at PR time.
- Whether `jazz-broker-wasm` should build with `wasm-bindgen`'s `--target web` or a
  hand-rolled loader; constraint is only "instantiable from embedded bytes inside a module
  SharedWorker".
- Phase 2 timing: same release as Phase 1 or separate. They are independent; the wire
  protocol does not change in either.
