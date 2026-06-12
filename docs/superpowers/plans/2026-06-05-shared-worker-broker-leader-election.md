# SharedWorker Broker Leader Election Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace browser persistent BroadcastChannel leader coordination with a SharedWorker broker and direct follower data ports into the leader worker.

**Architecture:** A new SharedWorker broker owns control-plane election, leadership IDs, lock monitoring, demotion, follower-port assignment, and storage reset. `Db.createWithWorker` uses a tab-side broker client; only the leader starts a dedicated OPFS worker, while followers use a main-thread runtime bridged over a broker-assigned `MessagePort`.

**Tech Stack:** TypeScript, Vitest browser tests, Web Locks, SharedWorker, MessageChannel, Rust `wasm-bindgen`, existing Jazz postcard worker sync protocol.

---

### Task 1: Protocol, Capability, And Lock Primitives

**Files:**

- Create: `packages/jazz-tools/src/runtime/browser-broker-protocol.ts`
- Modify: `packages/jazz-tools/src/runtime/leader-lock.ts`
- Test: `packages/jazz-tools/src/runtime/browser-broker-protocol.test.ts`
- Test: `packages/jazz-tools/tests/browser/leader-lock.test.ts`

- [ ] **Step 1: Write failing protocol tests**

Add tests for:

```ts
expect(formatUnsupportedBrowserBrokerError(["SharedWorker"])).toBe(
  "Jazz persistent browser mode requires SharedWorker, MessageChannel, and Web Locks support. This environment is missing: SharedWorker.",
);
expect(
  selectLeaderCandidate([
    { tabId: "a", visibility: "visible", lastVisibleAt: 10 },
    { tabId: "b", visibility: "visible", lastVisibleAt: 20 },
  ])?.tabId,
).toBe("b");
expect(
  selectLeaderCandidate([
    { tabId: "a", visibility: "hidden", lastVisibleAt: 10 },
    { tabId: "b", visibility: "hidden", lastVisibleAt: 20 },
  ])?.tabId,
).toBe("b");
expect(
  createBrowserBrokerFingerprint({
    appId: "app",
    dbName: "db",
    env: "dev",
    userBranch: "main",
    serverUrl: "ws://example.test",
    schemaHash: "schema-a",
    authClass: "user:stable-id",
    runtimeSourceIdentity: "default",
  }),
).not.toContain("jwt");
```

- [ ] **Step 2: Run protocol tests to verify RED**

Run: `pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/runtime/browser-broker-protocol.test.ts`

Expected: FAIL because `browser-broker-protocol.ts` does not exist.

- [ ] **Step 3: Implement protocol helpers**

Create exported types for broker messages, `BROKER_CONTROL_PROTOCOL_VERSION`, `formatUnsupportedBrowserBrokerError`, `detectBrowserBrokerMissingCapabilities`, `selectLeaderCandidate`, and `createBrowserBrokerFingerprint`. The fingerprint uses deterministic JSON with stable fields and excludes transient JWT/cookie strings.

- [ ] **Step 4: Run protocol tests to verify GREEN**

Run: `pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/runtime/browser-broker-protocol.test.ts`

Expected: PASS.

- [ ] **Step 5: Write failing lock tests**

Add browser tests for fail-fast acquisition returning `null` while held, abortable queued monitor grant after release, abort cancellation not reporting loss, and `{ steal: true }` stealing only when invoked.

- [ ] **Step 6: Run lock tests to verify RED**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/leader-lock.test.ts`

Expected: FAIL because the new monitor/steal helpers are missing.

- [ ] **Step 7: Implement lock helpers**

Extend `leader-lock.ts` with `tryAcquireWebLock`, `monitorWebLockRelease`, and `stealAndReleaseWebLock`. Remove the old navigator-lock strategy API once the broker no longer uses that abstraction.

- [ ] **Step 8: Run lock tests to verify GREEN**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/leader-lock.test.ts`

Expected: PASS.

### Task 2: SharedWorker Broker Control Plane

**Files:**

- Create: `packages/jazz-tools/src/worker/jazz-broker-worker.ts`
- Create: `packages/jazz-tools/src/runtime/browser-broker-client.ts`
- Test: `packages/jazz-tools/tests/browser/shared-worker-broker.test.ts`
- Modify: `packages/jazz-tools/package.json`

- [ ] **Step 1: Write failing broker browser tests**

Add tests that create two `BrowserBrokerClient` instances through the public client class and assert:

```ts
const first = await BrowserBrokerClient.connect(testOptions("a"));
const second = await BrowserBrokerClient.connect(testOptions("b"));
await first.waitForRole("leader");
await second.waitForRole("follower");
expect(first.snapshot().brokerInstanceId).toEqual(second.snapshot().brokerInstanceId);
expect(first.snapshot().leadershipId).toBe(1);
```

Add mismatch and unsupported tests:

```ts
await expect(BrowserBrokerClient.connect({ ...options, fingerprint: "different" })).rejects.toThrow(
  "incompatible persistent browser configuration",
);
```

- [ ] **Step 2: Run broker tests to verify RED**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/shared-worker-broker.test.ts`

Expected: FAIL because broker client and worker do not exist.

- [ ] **Step 3: Implement broker worker**

Implement the SharedWorker `connect` handler, `hello` validation, fingerprint establishment, broker instance generation, visibility updates, leader ranking, leadership ID increment, `become-leader`, `leader-ready`, `leader-failed`, `shutdown`, `broker-ping`, `broker-pong`, and `unsupported`. Add lock monitor state but leave data ports for Task 4.

- [ ] **Step 4: Implement broker client**

Implement `BrowserBrokerClient.connect`, hello handshake, message validation, stale broker instance/leadership ID filtering, role snapshots, visibility reporting, ping/pong, leader promotion callback hooks, demotion hooks, and shutdown.

- [ ] **Step 5: Include broker worker in package build**

Update `packages/jazz-tools/package.json` `build:runtime` to copy `src/worker/jazz-broker-worker.ts` alongside `jazz-worker.ts` after `tsc`.

- [ ] **Step 6: Run broker tests to verify GREEN**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/shared-worker-broker.test.ts`

Expected: PASS.

### Task 3: Worker Lock Preflight And Follower Port Rust Bridge

**Files:**

- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts`
- Modify: `packages/jazz-tools/src/runtime/worker-bridge.ts`
- Modify: `crates/jazz-wasm/src/runtime.rs`
- Modify: `crates/jazz-wasm/src/worker_host.rs`
- Modify: `crates/jazz-wasm/src/worker_protocol.rs`
- Test: `crates/jazz-wasm/tests/worker_bridge.rs`
- Test: `packages/jazz-tools/tests/browser/worker-bridge.test.ts`

- [ ] **Step 1: Write failing wasm bridge tests**

Add wasm tests that create a fake `MessagePort` target, attach it as a follower peer, send peer-bound runtime output, and assert the target receives a `WorkerToMainWire::PeerSync` for that peer. Add a detach test that closes the peer and removes the target.

- [ ] **Step 2: Run wasm tests to verify RED**

Run: `cargo test -p jazz-wasm worker_bridge`

Expected: FAIL because follower port attachment is not implemented.

- [ ] **Step 3: Implement direct peer port routing**

Extend worker-side peer routing so `peer_routing_lookup` can return `{ peerId, leadershipId, target }`. In `RustOutboxSender`, post peer-bound `WorkerToMainWire::PeerSync` to `target` when present, otherwise keep the existing main-thread target fallback.

- [ ] **Step 4: Add worker host attach/detach messages**

Teach `worker_host.rs` to accept JS object messages `{ type: "attach-follower-port", peerId, leadershipId, port }` and `{ type: "detach-follower-port", peerId, leadershipId }`. On attach, `start()` the port, map it to a peer client, decode incoming `MainToWorkerWire::Sync`/`PeerSync` from that port, and post `{ type: "follower-port-attached", peerId, leadershipId }` to the leader tab.

- [ ] **Step 5: Add TS bridge APIs**

Add `WorkerBridge.attachFollowerPort(peerId, leadershipId, port)` and `WorkerBridge.detachFollowerPort(peerId, leadershipId)`. Add a `MessagePortRuntimeBridge` for follower main runtimes that installs the runtime sender on a `MessagePort`, decodes incoming worker payloads, and exposes `shutdown()`.

- [ ] **Step 6: Add worker lock preflight**

Update `jazz-worker.ts` init handling so when init includes `workerLockName`, it acquires that Web Lock with fail-fast semantics before calling `runAsWorker`. If missing, keep existing behavior for non-brokered tests. If unavailable or busy, post explicit `{ type: "error" }`.

- [ ] **Step 7: Run wasm and worker tests to verify GREEN**

Run:

```bash
cargo test -p jazz-wasm worker_bridge
pnpm --filter jazz-tools test:browser -- tests/browser/worker-bridge.test.ts
```

Expected: PASS for touched tests.

### Task 4: Integrate Broker Into Db Persistent Browser Mode

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`
- Modify: `packages/jazz-tools/src/runtime/db.worker-bootstrap.test.ts`
- Test: `packages/jazz-tools/tests/browser/worker-bridge.test.ts`

- [ ] **Step 1: Write failing browser integration assertions**

Update browser tests to assert two persistent `createDb` calls for the same namespace result in exactly one dedicated worker. Add assertions that follower `Db` instances have `tabRole === "follower"` and no `worker` field, while writes from the follower still persist through the leader.

- [ ] **Step 2: Run integration tests to verify RED**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/worker-bridge.test.ts`

Expected: FAIL because followers still spawn fallback workers.

- [ ] **Step 3: Replace persistent startup path**

In `Db.createWithWorker`, create `BrowserBrokerClient` for persistent browser mode, fail fast if required APIs are missing, and remove `TabLeaderElection` startup. Set `primaryDbName` to the resolved namespace. Do not compute fallback worker namespaces.

- [ ] **Step 4: Promote leaders through broker callbacks**

On `become-leader`, acquire the tab lock, spawn worker, attach bridge on first schema, include `workerLockName`, and report `leader-ready` only after bridge init resolves.

- [ ] **Step 5: Start followers without workers**

When role is follower, keep `worker` null, create main-thread non-durable clients with binary encoding, wait for broker follower data port assignment, attach `MessagePortRuntimeBridge`, and resolve durable waits after `follower-ready`.

- [ ] **Step 6: Demote and shutdown cleanly**

On demotion or shutdown, poison stale leadership ID state, close follower data ports, shut down bridge/client resources, terminate workers, release the tab lock lease, and notify broker.

- [ ] **Step 7: Run integration tests to verify GREEN**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/worker-bridge.test.ts`

Expected: PASS for leader/follower routing and failover tests.

### Task 5: Broker Lock Monitoring, Forced Takeover, And Epoch Restart

**Files:**

- Modify: `packages/jazz-tools/src/worker/jazz-broker-worker.ts`
- Modify: `packages/jazz-tools/src/runtime/browser-broker-client.ts`
- Test: `packages/jazz-tools/tests/browser/shared-worker-broker.test.ts`
- Test: `packages/jazz-tools/tests/browser/worker-bridge.test.ts`

- [ ] **Step 1: Write failing failover tests**

Add tests for leader tab-lock release electing the most recently visible tab, worker-lock release electing a replacement, planned demotion not reporting unexpected lock loss, hidden-only election choosing newest `lastVisibleAt`, and forced lock stealing after a short configured `forceTakeoverTimeoutMs`.

- [ ] **Step 2: Run failover tests to verify RED**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/shared-worker-broker.test.ts tests/browser/worker-bridge.test.ts`

Expected: FAIL because lock monitors and forced takeover are incomplete.

- [ ] **Step 3: Implement monitor lifecycle**

After `leader-ready`, start abortable queued lock monitors for tab and worker locks. Cancel and mark planned releases before demotion, reset, shutdown, or replacement election. On unexpected grant for the current leadership ID, release immediately, mark that leadership ID dead, close follower ports, and elect.

- [ ] **Step 4: Implement forced takeover**

Track demotion deadlines. If a lock remains held past `forceTakeoverTimeoutMs`, call `stealAndReleaseWebLock` for that lock and continue election.

- [ ] **Step 5: Implement broker liveness and broker instance handling**

Send periodic `broker-ping`, require a `broker-pong` for the current broker instance, and make tabs poison stale local state when the broker instance changes or broker liveness fails before reconnecting.

- [ ] **Step 6: Run failover tests to verify GREEN**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/shared-worker-broker.test.ts tests/browser/worker-bridge.test.ts`

Expected: PASS.

### Task 6: Brokered Storage Reset

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`
- Modify: `packages/jazz-tools/src/runtime/browser-broker-client.ts`
- Modify: `packages/jazz-tools/src/worker/jazz-broker-worker.ts`
- Remove or stop importing: `packages/jazz-tools/src/runtime/storage-reset-coordinator.ts`
- Test: `packages/jazz-tools/tests/browser/worker-bridge.test.ts`

- [ ] **Step 1: Write failing reset test**

Update the follower-initiated storage wipe test to assert no fallback namespace is created and reset reconnects all tabs through the broker after clearing primary OPFS storage.

- [ ] **Step 2: Run reset test to verify RED**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/worker-bridge.test.ts -t "deletes OPFS storage"`

Expected: FAIL while reset still depends on BroadcastChannel coordinator or fallback namespaces.

- [ ] **Step 3: Implement reset protocol**

Route `deleteClientStorage()` to broker `storage-reset-request`. Broker freezes follower attachments, sends close-port messages, demotes current leader, waits for lock release, elects reset coordinator, starts worker in reset mode, waits for `leader-ready`, reconnects followers, and sends `storage-reset-finished`.

- [ ] **Step 4: Implement reset worker mode**

Pass reset mode into worker bridge options so the worker deletes the primary namespace before opening a clean persistent runtime. Remove fallback namespace cleanup.

- [ ] **Step 5: Run reset test to verify GREEN**

Run: `pnpm --filter jazz-tools test:browser -- tests/browser/worker-bridge.test.ts -t "deletes OPFS storage"`

Expected: PASS.

### Task 7: Remove Old Coordination Paths And Verify

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`
- Remove or leave unused for compatibility only: `packages/jazz-tools/src/runtime/tab-leader-election.ts`
- Remove or leave unused for compatibility only: `packages/jazz-tools/src/runtime/tab-sync-protocol.ts`
- Modify: `packages/jazz-tools/tests/browser/tab-leader-election.test.ts`
- Modify: `packages/jazz-tools/src/runtime/tab-leader-election.test.ts`
- Modify: `packages/jazz-tools/bin/docs-index.txt` only if generated docs tests require it

- [ ] **Step 1: Remove old imports and runtime usage**

Remove `TabLeaderElection`, `tab-sync-protocol`, `StorageResetCoordinator`, BroadcastChannel sync channel fields, follower fallback namespace resolution, and related handlers from `db.ts`.

- [ ] **Step 2: Update obsolete tests**

Remove or replace tests that assert BroadcastChannel fallback behavior. Keep behavior tests that still apply through the broker.

- [ ] **Step 3: Run focused tests**

Run:

```bash
pnpm --filter jazz-tools exec vitest run --config vitest.config.ts src/runtime/browser-broker-protocol.test.ts src/runtime/db.worker-bootstrap.test.ts
pnpm --filter jazz-tools test:browser -- tests/browser/leader-lock.test.ts tests/browser/shared-worker-broker.test.ts tests/browser/worker-bridge.test.ts
cargo test -p jazz-wasm worker_bridge
```

Expected: PASS.

- [ ] **Step 4: Run core build**

Run: `pnpm build:core`

Expected: PASS.

- [ ] **Step 5: Run package/browser tests if time allows**

Run: `pnpm --filter jazz-tools test`

Expected: PASS or report any unrelated pre-existing failures with exact output.
