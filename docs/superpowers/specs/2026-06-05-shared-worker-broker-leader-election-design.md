# SharedWorker Broker Leader Election Design

## Scope

Implement the MVP in `specs/todo/a_mvp/shared_worker_broker_leader_election.md` for browser persistent mode. The old BroadcastChannel tab election, BroadcastChannel follower sync, BroadcastChannel storage reset coordinator, and follower fallback OPFS namespaces are replaced by a SharedWorker control-plane broker plus direct follower `MessagePort` data paths into the leader dedicated worker.

Memory-mode browser clients, Node/NAPI clients, and the public `createDb` API shape are unchanged.

## Architecture

The browser persistent runtime gains one SharedWorker broker per `{appId, dbName}` namespace. The broker owns connected tab state, broker instance, visibility ranking, leadership IDs, lock monitoring, demotion, follower-port assignment, and storage reset coordination. Tabs interact with it through a small control protocol implemented by a new `BrowserBrokerClient`.

The leader tab acquires `jazz-leader-tab:<appId>:<dbName>`, then starts the existing dedicated worker. The worker init preflight acquires `jazz-leader-worker:<appId>:<dbName>` before Rust opens OPFS. Followers do not spawn dedicated workers and do not use fallback namespaces.

Follower sync uses transferred `MessagePort`s. The broker creates a channel per follower attachment, sends one endpoint to the follower, and sends the other endpoint to the leader tab for transfer into the leader worker. The Rust wasm worker host maps the transferred endpoint to a peer client. The existing postcard worker sync envelopes remain the payload format on the data ports, but the broker never parses or relays those payloads.

## Components

- `packages/jazz-tools/src/runtime/browser-broker-protocol.ts`: shared message types, protocol constants, validation helpers, unsupported-environment error formatting, configuration fingerprint creation, and visibility ranking.
- `packages/jazz-tools/src/runtime/browser-broker-client.ts`: tab-side SharedWorker connection, broker instance/leadership ID fencing, leader promotion/demotion handling, follower port lifecycle, storage reset request handling, ping/pong, and shutdown.
- `packages/jazz-tools/src/worker/jazz-broker-worker.ts`: SharedWorker broker entry point. Owns registry, election, leadership IDs, lock monitors, follower port assignment, reset orchestration, and forced lock stealing after timeout.
- `packages/jazz-tools/src/runtime/leader-lock.ts`: expanded Web Locks helpers for fail-fast acquisition, abortable queued monitor requests, and exceptional steal-and-release.
- `packages/jazz-tools/src/runtime/db.ts`: replace `TabLeaderElection`, `tab-sync-protocol`, `StorageResetCoordinator`, and follower fallback namespace usage in persistent browser mode with the broker client.
- `packages/jazz-tools/src/runtime/worker-bridge.ts`: expose follower port attachment on the leader bridge and message-port bridge attachment on follower main runtimes.
- `packages/jazz-tools/src/worker/jazz-worker.ts`: acquire worker Web Lock during init before handing off to Rust.
- `crates/jazz-wasm/src/worker_host.rs` and `crates/jazz-wasm/src/runtime.rs`: accept follower `MessagePort`s in the leader worker, route peer-bound payloads to the correct port, and close ports on detach.
- `packages/jazz-tools/tests/browser/worker-bridge.test.ts` and focused runtime tests: black-box browser coverage for persistent behavior.

## Data Flow

Leader startup:

1. `Db.createWithWorker` creates a broker client and sends `hello`.
2. Broker elects a candidate by visibility ranking and sends `become-leader`.
3. Candidate acquires the tab lock, starts the dedicated worker, and waits for bridge initialization.
4. Worker init acquires the worker Web Lock before OPFS opens.
5. Leader reports `leader-ready`.
6. Broker starts queued lock monitors and announces durable-ready leadership.

Follower startup:

1. Follower creates only its main-thread runtime.
2. Broker waits for a durable-ready leader.
3. Broker creates a `MessageChannel` for that leadership ID.
4. Leader transfers its endpoint to the worker.
5. Worker acknowledges the follower port.
6. Broker sends `follower-ready`; follower startup and durable waits can proceed.

Follower payloads travel directly between the follower main runtime and leader worker over the assigned data port. Broker control messages never share that port.

## Failure Handling

Broker-to-tab messages include `brokerInstanceId`; leader-specific messages include `leadershipId`. Tabs and workers ignore stale broker instances/leadership IDs and close stale follower ports.

The broker monitors the leader tab lock and worker lock with abortable queued lock requests. If either monitor is granted for the current unplanned leadership ID, the broker marks the leadership ID dead, asks tabs to close follower ports, and elects a replacement. Planned demotion, shutdown, and reset cancel monitors before release.

If demotion does not release a lock within `forceTakeoverTimeoutMs`, the broker uses Web Locks `{ steal: true }`, releases the stolen lock immediately, and continues election. It does not treat stealing as proof that old JavaScript stopped; replacement worker OPFS open failures remain explicit stale-worker/open failures.

## Storage Reset

`deleteClientStorage()` sends `storage-reset-request` to the broker. The broker freezes follower assignment, closes current follower ports, demotes the leader, waits for locks to release with forced takeover if needed, elects a reset leader, and starts that worker in reset mode. The reset worker deletes the primary OPFS namespace before opening a fresh persistent runtime. The broker reconnects followers and reports reset completion.

## Testing

Tests follow the repo instruction to prefer black-box browser integration and public API setup. Existing behavior tests are updated only where their old assertions encode replaced BroadcastChannel/fallback implementation details.

Focused unit tests cover protocol validation, unsupported-environment error text, deterministic fingerprints, and visibility ranking. Browser tests cover unsupported API failure, single leader worker across tabs, follower no-worker startup, follower-port acknowledgement gating, follower read/write persistence through the leader worker, lock-loss failover, planned demotion/reset monitor cancellation, forced lock stealing, broker instance changes after broker restart, hidden-only ranking, and follower-initiated storage reset.
