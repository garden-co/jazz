/**
 * SharedWorker entry script for the Jazz Safari leader.
 *
 * Boot sequence:
 *   1. Accept SharedWorker connections; validate CONNECT.
 *   2. On the first valid CONNECT (or a CHECK_CAPABILITY probe), run the
 *      capability probe.
 *   3. If supported, instantiate SharedWorkerLeader and bootstrap the runtime
 *      (Task 12). Otherwise, post LEADER_FAULT/runtime-host-unavailable.
 *   4. Mint a follower MessagePort for the tab and reply with PEER_PORT.
 */

import {
  LEADER_PROTOCOL_VERSION,
  isTabToLeader,
  type ConnectMessage,
  type LeaderFaultReason,
  type LeaderToTab,
  type TabId,
} from "./protocol.js";
import { detectSyncOpfsInWorkerScope } from "./capability.js";
import { createSharedWorkerLeader, type LeaderHost } from "./leader-host.js";

// `SharedWorkerGlobalScope` is a worker-only lib type; this package compiles
// against the `dom` lib (see jazz-worker.ts for the same pattern). Declare the
// minimal structural surface we use instead of referencing the global type.
declare const self: {
  onconnect: ((event: MessageEvent) => void) | null;
};

interface TabRecord {
  id: TabId;
  port: MessagePort;
  scope: string;
}

const tabs = new Map<TabId, TabRecord>();
let leaderScope: string | null = null;
let leaderHost: LeaderHost | null = null;
let initPromise: Promise<void> | null = null;
let initError: { reason: LeaderFaultReason; detail?: string } | null = null;

function postToTab(record: TabRecord, msg: LeaderToTab, transfer?: Transferable[]): void {
  try {
    if (transfer && transfer.length > 0) {
      record.port.postMessage(msg, transfer);
    } else {
      record.port.postMessage(msg);
    }
  } catch {
    // port likely closed
  }
}

async function ensureLeaderReady(hello: ConnectMessage): Promise<void> {
  if (initError) throw new Error(initError.reason);
  if (leaderHost) return;
  if (!initPromise) {
    initPromise = (async () => {
      const supported = await detectSyncOpfsInWorkerScope();
      if (!supported) {
        initError = { reason: "runtime-host-unavailable" };
        throw new Error(initError.reason);
      }
      try {
        const host = createSharedWorkerLeader({
          scope: hello.scope,
          appId: hello.appId,
          dbName: hello.dbName,
          schemaJson: hello.schemaJson,
          env: hello.env,
          userBranch: hello.userBranch,
          serverUrl: hello.serverUrl,
          jwtToken: hello.jwtToken,
          adminSecret: hello.adminSecret,
        });
        await host.init();
        leaderHost = host;
      } catch (err) {
        // `init-failed` is a deliberate catch-all bucket (lock-acquisition,
        // wasm-init, init-timeout all collapse to it). Preserve the real stack
        // in the SharedWorker's devtools context — the tab only receives the
        // string `detail`, which loses the stack.
        console.error("[jazz] shared-worker leader init failed:", err);
        initError = {
          reason: "init-failed",
          detail: err instanceof Error ? err.message : String(err),
        };
        throw err;
      }
    })();
  }
  await initPromise;
}

async function handleConnect(record: TabRecord, msg: ConnectMessage): Promise<void> {
  if (msg.protocolVersion !== LEADER_PROTOCOL_VERSION) {
    postToTab(record, { t: "LEADER_FAULT", reason: "version-mismatch" });
    return;
  }
  // Defense-in-depth: unreachable under correct URL/name resolution, since
  // buildLeaderWorkerName(scope) routes distinct scopes to distinct SharedWorker
  // instances by name — two scopes can never reach the same instance. Kept as a
  // cheap guard against a future naming refactor that breaks that invariant.
  if (leaderScope && msg.scope !== leaderScope) {
    postToTab(record, { t: "LEADER_FAULT", reason: "scope-mismatch" });
    return;
  }
  leaderScope = leaderScope ?? msg.scope;
  record.scope = msg.scope;
  record.id = msg.tabId;
  tabs.set(record.id, record);

  try {
    await ensureLeaderReady(msg);
  } catch {
    postToTab(record, {
      t: "LEADER_FAULT",
      reason: initError?.reason ?? "init-failed",
      detail: initError?.detail,
    });
    return;
  }

  if (!leaderHost) {
    postToTab(record, { t: "LEADER_FAULT", reason: "init-failed" });
    return;
  }
  const { followerPort, generation } = await leaderHost.attachFollower(record.id);
  postToTab(record, { t: "PEER_PORT", port: followerPort, generation }, [followerPort]);
}

function handleGoodbye(record: TabRecord): void {
  if (!record.id) return;
  tabs.delete(record.id);
  leaderHost?.detachFollower(record.id);
  try {
    record.port.close();
  } catch {
    // ignored
  }
}

let cachedCapability: boolean | null = null;

async function answerCapability(record: TabRecord): Promise<void> {
  if (cachedCapability === null) {
    try {
      cachedCapability = await detectSyncOpfsInWorkerScope();
    } catch {
      cachedCapability = false;
    }
  }
  postToTab(record, { t: "CAPABILITY_RESULT", supported: cachedCapability });
}

self.onconnect = (event: MessageEvent) => {
  const port = event.ports[0];
  if (!port) return;
  const record: TabRecord = { id: "", port, scope: "" };
  port.onmessage = (msg: MessageEvent) => {
    if (!isTabToLeader(msg.data)) return;
    const data = msg.data;
    switch (data.t) {
      case "CHECK_CAPABILITY":
        void answerCapability(record);
        return;
      case "CONNECT":
        void handleConnect(record, data);
        return;
      case "GOODBYE":
        handleGoodbye(record);
        return;
    }
  };
  port.start();
};
