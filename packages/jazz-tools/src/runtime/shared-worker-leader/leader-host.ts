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

/**
 * Hosts the in-process Jazz runtime inside the SharedWorker scope. Acquires
 * LOCK_NAME, opens OPFS, opens upstream WebSocket, and attaches follower
 * MessagePorts directly to its own PEER_ROUTING table via the Rust
 * worker-host's `attach-follower-port` handler.
 *
 * Real runtime hosting lands in Task 12.
 */
export function createSharedWorkerLeader(_options: SharedWorkerLeaderOptions): LeaderHost {
  return {
    async init(): Promise<void> {
      throw new Error("SharedWorkerLeader.init not yet implemented (Task 12)");
    },
    async attachFollower(_tabId: TabId) {
      throw new Error("SharedWorkerLeader.attachFollower not yet implemented (Task 12)");
    },
    detachFollower(_tabId: TabId): void {
      throw new Error("SharedWorkerLeader.detachFollower not yet implemented (Task 12)");
    },
  };
}
