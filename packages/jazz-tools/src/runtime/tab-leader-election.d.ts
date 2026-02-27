import { type LeaderLockStrategy } from "./leader-lock.js";
/**
 * Cross-tab leader election for browser tabs using BroadcastChannel.
 *
 * Lifecycle (per tab):
 *
 *   start
 *     |
 *     +--> requestCurrentLeader() ------------------------------+
 *     |                                                        |
 *     +--> tryAcquireLeadershipLock()                         |
 *            |                                                |
 *            +--> acquired -> leader                          |
 *            |       |                                        |
 *            |       +--> claim + heartbeat                   |
 *            |       +--> keep lock lease until step-down     |
 *            |                                                |
 *            +--> not acquired -> follower                    |
 *                    |                                        |
 *                    +--> wait for heartbeat/claim -----------+
 *                    |
 *                    +--> lease timeout -> tryAcquireLeadershipLock() again
 *
 * Election model:
 * - Lock ownership determines who may become leader.
 * - Leader messages still use term + leader ID fencing.
 * - Followers discover leader over BroadcastChannel and re-probe on timeout.
 */
export type LeaderRole = "leader" | "follower";
export interface LeaderSnapshot {
  role: LeaderRole;
  tabId: string;
  leaderTabId: string | null;
  term: number;
}
export interface TabLeaderElectionOptions {
  appId: string;
  dbName: string;
  heartbeatMs?: number;
  leaseMs?: number;
  tabId?: string;
  now?: () => number;
  lockStrategy?: LeaderLockStrategy;
}
type LeaderChangeListener = (snapshot: LeaderSnapshot) => void;
export declare class TabLeaderElection {
  private readonly tabId;
  private readonly heartbeatMs;
  private readonly leaseMs;
  private readonly now;
  private readonly channelName;
  private readonly lockName;
  private readonly lockStrategy;
  private started;
  private channel;
  private role;
  private term;
  private leaderTabId;
  private lastLeaderSeenAtMs;
  private heartbeatTimer;
  private leaseDeadlineTimer;
  private probeInFlight;
  private leadershipLockLease;
  private readonly listeners;
  private readyResolve;
  private readyReject;
  private readonly readyPromise;
  private readySettled;
  private readonly onMessage;
  constructor(options: TabLeaderElectionOptions);
  start(): void;
  stop(): void;
  onChange(listener: LeaderChangeListener): () => void;
  snapshot(): LeaderSnapshot;
  isLeader(): boolean;
  waitForInitialLeader(timeoutMs?: number): Promise<LeaderSnapshot>;
  private handleIncomingMessage;
  private handleLeaderHeartbeat;
  private handleLeaderClaim;
  private promoteToLeader;
  private setLeader;
  private ensureHeartbeatTimer;
  private clearHeartbeatTimer;
  private scheduleLeaseDeadlineCheck;
  private clearLeaseDeadlineTimer;
  private onLeaseDeadline;
  private sendHeartbeat;
  private postMessage;
  private requestCurrentLeader;
  private tryTakeLeadership;
  private tryAcquireLeadershipLock;
  private releaseLeadershipLock;
  private emitChange;
  private resolveReadyIfNeeded;
}

//# sourceMappingURL=tab-leader-election.d.ts.map
