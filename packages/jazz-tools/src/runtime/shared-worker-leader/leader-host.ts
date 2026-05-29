import type { TabId } from "./protocol.js";

export interface LeaderHost {
  init(): Promise<void>;
  attachFollower(tabId: TabId): Promise<{
    followerPort: MessagePort;
    generation: number;
  }>;
  detachFollower(tabId: TabId): void;
}

export interface SharedWorkerLeaderOptions {
  scope: string;
  appId: string;
  dbName: string;
  schemaJson: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  clientId?: string;
}

function buildLockName(appId: string, dbName: string): string {
  return `jazz-worker:${appId}:${dbName}`;
}

async function acquireExclusiveLock(name: string): Promise<void> {
  const nav = (self as unknown as { navigator: { locks: LockManager } }).navigator;
  return new Promise<void>((resolveAcquired, rejectAcquired) => {
    nav.locks
      .request(name, { mode: "exclusive" }, () => {
        resolveAcquired();
        // Held until SharedWorker termination — never resolves.
        return new Promise<void>(() => {});
      })
      .catch((err) => rejectAcquired(err));
  });
}

export function createSharedWorkerLeader(options: SharedWorkerLeaderOptions): LeaderHost {
  let runtimeInitialized = false;
  // Reserved. Always 1 in v1: a fresh SharedWorker boot resets module scope, so
  // there is no persistent cross-reboot counter. The Rust PEER_ROUTING keys on
  // peer_id (not generation), so reattach with generation 1 is harmless. This
  // field becomes load-bearing only in the tab-hosted follow-up plan, which
  // needs it to reject stale ATTACH_PORT_ACKs across leader generations; that
  // plan adds the persistent counter. Do not invent one here.
  const generation = 1;
  const attachedFollowers = new Set<TabId>();

  async function bootstrapRuntime(): Promise<void> {
    if (runtimeInitialized) return;
    if (!options.appId || !options.dbName) {
      throw new Error("shared-worker-leader: appId + dbName required");
    }

    await acquireExclusiveLock(buildLockName(options.appId, options.dbName));

    const wasmModule: typeof import("jazz-wasm") =
      (await import("jazz-wasm")) as unknown as typeof import("jazz-wasm");
    if (
      typeof (wasmModule as unknown as { default?: () => Promise<unknown> }).default === "function"
    ) {
      await (wasmModule as unknown as { default: () => Promise<unknown> }).default();
    }

    const init = {
      type: "init" as const,
      schemaJson: options.schemaJson,
      appId: options.appId,
      env: options.env ?? "default",
      userBranch: options.userBranch ?? "main",
      dbName: options.dbName,
      clientId: options.clientId ?? crypto.randomUUID(),
      serverUrl: options.serverUrl,
      jwtToken: options.jwtToken,
      adminSecret: options.adminSecret,
      runtimeSources: undefined,
    };

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("init-ok timeout")), 15000);
      const onInitOk = () => {
        clearTimeout(timeout);
        resolve();
      };
      try {
        (
          wasmModule as unknown as {
            runAsWorker(init: unknown, pending: unknown[], onInitOk: () => void): void;
          }
        ).runAsWorker(init, [], onInitOk);
      } catch (err) {
        clearTimeout(timeout);
        reject(err);
      }
    });

    runtimeInitialized = true;
  }

  function dispatchToRustOnMessage(data: unknown, ports: MessagePort[]): void {
    const event = new MessageEvent("message", { data, ports });
    const handler = (self as unknown as { onmessage?: (ev: MessageEvent) => void }).onmessage;
    if (typeof handler === "function") {
      handler(event);
    } else {
      throw new Error("shared-worker-leader: Rust onmessage handler is not installed");
    }
  }

  return {
    async init(): Promise<void> {
      await bootstrapRuntime();
    },
    async attachFollower(tabId: TabId) {
      if (!runtimeInitialized) {
        throw new Error("shared-worker-leader: init() must complete before attachFollower");
      }
      const mc = new MessageChannel();
      dispatchToRustOnMessage(
        {
          type: "attach-follower-port",
          followerTabId: tabId,
          leaderTabId: "shared-worker-leader",
          generation,
        },
        [mc.port1],
      );
      attachedFollowers.add(tabId);
      return {
        followerPort: mc.port2,
        generation,
      };
    },
    detachFollower(tabId: TabId): void {
      attachedFollowers.delete(tabId);
      // PEER_ROUTING entry stays until SharedWorker shutdown; the closed
      // MessagePort already signals the leader side. Active cleanup is a
      // follow-up if we observe leaked clients.
    },
  };
}
