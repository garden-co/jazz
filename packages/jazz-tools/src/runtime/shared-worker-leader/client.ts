import {
  LEADER_PROTOCOL_VERSION,
  buildLeaderScope,
  buildLeaderWorkerName,
  isLeaderToTab,
  type ConnectMessage,
  type LeaderFaultReason,
  type LeaderToTab,
  type TabId,
} from "./protocol.js";

export interface SharedWorkerLeaderClientOptions {
  appId: string;
  dbName: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  jazzPackageVersion: string;
  leaderUrl: string;
  tabId: TabId;
  bornAt: number;
}

/**
 * Per-call arg for connect(). schemaJson reaches Db lazily (only when the user
 * touches a schema), so it is not on construction options.
 */
export interface SharedWorkerLeaderConnectArgs {
  schemaJson: string;
}

export interface PeerPortSnapshot {
  port: MessagePort;
  generation: number;
}

/**
 * Thrown when CONNECT fails after a positive capability probe. By explicit
 * design there is no dedicated-Worker fallback in this case — a supported but
 * broken leader is a hard error. `reason` carries the LeaderFaultReason when
 * the failure was a LEADER_FAULT; otherwise it is "connect-timeout" or
 * "connect-post-failed".
 */
export class SharedWorkerLeaderConnectError extends Error {
  constructor(
    readonly reason: LeaderFaultReason | "connect-timeout" | "connect-post-failed",
    readonly detail?: string,
  ) {
    super(`SharedWorker leader connect failed: ${reason}${detail ? ` (${detail})` : ""}`);
    this.name = "SharedWorkerLeaderConnectError";
  }
}

export interface SharedWorkerLeaderClient {
  /**
   * Sends CHECK_CAPABILITY and resolves with the supported flag from the
   * leader. Cheap; no runtime bootstrap is triggered. Safe to call before any
   * schema is known. Resolves to false on timeout (2s).
   */
  checkCapability(): Promise<boolean>;
  /**
   * Sends CONNECT with the given schemaJson and resolves with the first
   * PEER_PORT. Must be called after a positive checkCapability(). The client
   * does not enforce ordering — calling connect() without first probing is
   * legal but will time out on browsers without sync OPFS in SharedWorker.
   */
  connect(args: SharedWorkerLeaderConnectArgs): Promise<PeerPortSnapshot>;
  current(): PeerPortSnapshot | null;
  onPortChanged(cb: (snapshot: PeerPortSnapshot) => void): () => void;
  onFault(cb: (reason: LeaderFaultReason, detail?: string) => void): () => void;
  /** Closes the current control port and reconnects with a fresh CONNECT. */
  forceReconnect(): void;
  /** Sends GOODBYE and tears down the connection port. */
  close(): void;
}

export function createSharedWorkerLeaderClient(
  options: SharedWorkerLeaderClientOptions,
): SharedWorkerLeaderClient {
  const scope = buildLeaderScope(options.appId, options.dbName);
  const name = buildLeaderWorkerName(scope);

  const portListeners = new Set<(snapshot: PeerPortSnapshot) => void>();
  const faultListeners = new Set<(reason: LeaderFaultReason, detail?: string) => void>();
  let currentSnapshot: PeerPortSnapshot | null = null;
  let lastConnectArgs: SharedWorkerLeaderConnectArgs | null = null;
  let resolveFirst: ((snapshot: PeerPortSnapshot) => void) | null = null;
  let rejectFirst: ((err: Error) => void) | null = null;
  let firstPort = new Promise<PeerPortSnapshot>((resolve, reject) => {
    resolveFirst = resolve;
    rejectFirst = reject;
  });
  let capabilityResolve: ((supported: boolean) => void) | null = null;

  let worker: SharedWorker | null = null;
  let activePort: MessagePort | null = null;
  let closed = false;

  function buildConnectMessage(schemaJson: string): ConnectMessage {
    return {
      t: "CONNECT",
      tabId: options.tabId,
      bornAt: options.bornAt,
      scope,
      protocolVersion: LEADER_PROTOCOL_VERSION,
      jazzPackageVersion: options.jazzPackageVersion,
      appId: options.appId,
      dbName: options.dbName,
      schemaJson,
      env: options.env,
      userBranch: options.userBranch,
      serverUrl: options.serverUrl,
      jwtToken: options.jwtToken,
      adminSecret: options.adminSecret,
    };
  }

  function openConnection(): void {
    if (closed) return;
    try {
      worker = new SharedWorker(options.leaderUrl, { type: "module", name });
    } catch (err) {
      rejectFirst?.(new Error(`SharedWorker construction failed: ${(err as Error).message}`));
      capabilityResolve?.(false);
      capabilityResolve = null;
      return;
    }
    const port = worker.port;
    activePort = port;
    port.onmessage = (event: MessageEvent) => {
      const data = event.data;
      if (!isLeaderToTab(data)) return;
      switch ((data as LeaderToTab).t) {
        case "CAPABILITY_RESULT": {
          const supported = (data as Extract<LeaderToTab, { t: "CAPABILITY_RESULT" }>).supported;
          capabilityResolve?.(supported);
          capabilityResolve = null;
          return;
        }
        case "PEER_PORT": {
          const { port: peerPort, generation } = data as Extract<LeaderToTab, { t: "PEER_PORT" }>;
          currentSnapshot = { port: peerPort, generation };
          if (resolveFirst) {
            resolveFirst(currentSnapshot);
            resolveFirst = null;
            rejectFirst = null;
          }
          for (const cb of portListeners) cb(currentSnapshot);
          return;
        }
        case "LEADER_FAULT": {
          const fault = data as Extract<LeaderToTab, { t: "LEADER_FAULT" }>;
          // runtime-host-unavailable is the "fall back to dedicated worker"
          // signal and is only expected before a CONNECT (during the probe).
          // Any other fault after CONNECT is a hard error.
          if (rejectFirst) {
            rejectFirst(new SharedWorkerLeaderConnectError(fault.reason, fault.detail));
            resolveFirst = null;
            rejectFirst = null;
          }
          capabilityResolve?.(false);
          capabilityResolve = null;
          for (const cb of faultListeners) cb(fault.reason, fault.detail);
          return;
        }
      }
    };
    port.start();
  }

  openConnection();

  return {
    async checkCapability(): Promise<boolean> {
      return new Promise<boolean>((resolve) => {
        // 2s: the probe is sub-100ms once the worker is warm; this only needs
        // to cover ES-module parse + first detectSyncOpfsInWorkerScope on a
        // cold SharedWorker boot. Tighten/raise only if CI shows cold boots
        // exceeding it.
        const timeout = setTimeout(() => {
          capabilityResolve = null;
          resolve(false);
        }, 2000);
        capabilityResolve = (supported) => {
          clearTimeout(timeout);
          resolve(supported);
        };
        try {
          activePort?.postMessage({ t: "CHECK_CAPABILITY" });
        } catch {
          capabilityResolve = null;
          clearTimeout(timeout);
          resolve(false);
        }
      });
    },
    async connect(args: SharedWorkerLeaderConnectArgs): Promise<PeerPortSnapshot> {
      lastConnectArgs = args;
      try {
        activePort?.postMessage(buildConnectMessage(args.schemaJson));
      } catch (err) {
        throw new SharedWorkerLeaderConnectError("connect-post-failed", (err as Error).message);
      }
      // Bound the wait: a leader stuck acquiring LOCK_NAME or booting WASM must
      // not hang the caller forever. On timeout, reject firstPort so awaiters
      // get the typed error rather than a silent stall.
      const connectTimeout = setTimeout(() => {
        if (rejectFirst) {
          rejectFirst(new SharedWorkerLeaderConnectError("connect-timeout"));
          resolveFirst = null;
          rejectFirst = null;
        }
      }, 10000);
      try {
        const snap = await firstPort;
        clearTimeout(connectTimeout);
        return snap;
      } catch (err) {
        clearTimeout(connectTimeout);
        throw err;
      }
    },
    current() {
      return currentSnapshot;
    },
    onPortChanged(cb) {
      portListeners.add(cb);
      return () => portListeners.delete(cb);
    },
    onFault(cb) {
      faultListeners.add(cb);
      return () => faultListeners.delete(cb);
    },
    forceReconnect() {
      if (closed) return;
      try {
        activePort?.postMessage({ t: "GOODBYE" });
      } catch {
        // ignored
      }
      try {
        activePort?.close();
      } catch {
        // ignored
      }
      // No explicit `activePort = null` here: the old port is already closed
      // above, and openConnection() overwrites activePort + worker. Nulling
      // them first confuses tsc's control-flow narrowing (the closure
      // reassignment isn't credited at the call site).
      firstPort = new Promise<PeerPortSnapshot>((resolve, reject) => {
        resolveFirst = resolve;
        rejectFirst = reject;
      });
      openConnection();
      // Reissue the last CONNECT so the new SharedWorker boot recovers state.
      if (lastConnectArgs) {
        try {
          activePort?.postMessage(buildConnectMessage(lastConnectArgs.schemaJson));
        } catch {
          // best-effort; next caller's connect() will retry
        }
      }
    },
    close() {
      closed = true;
      try {
        activePort?.postMessage({ t: "GOODBYE" });
      } catch {
        // ignored
      }
      try {
        activePort?.close();
      } catch {
        // ignored
      }
      activePort = null;
      worker = null;
    },
  };
}
