/**
 * Cross-tab leader election for browser tabs using BroadcastChannel.
 *
 * Lifecycle (per tab):
 *
 *   start
 *     |
 *     +--> request current leader
 *     +--> arm initial-election timeout
 *     |
 *     +--> receive heartbeat/claim --------> follower (lease deadline armed)
 *     |                                         |
 *     |                                         +--> lease deadline expires
 *     |                                               without heartbeat
 *     |                                               |
 *     +-----------------------------------------------+--> promote self to leader
 *                                                           |
 *                                                           +--> emit claim + heartbeat
 *                                                           +--> send periodic heartbeats
 *
 * Election model:
 * - Each tab has a stable random tab ID.
 * - Leaders send periodic heartbeats with term + leader ID.
 * - Followers start an election when the lease deadline expires.
 * - Higher term always wins; same-term ties are broken by tab ID.
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
  initialElectionDelayMs?: number;
  tabId?: string;
  now?: () => number;
}

interface LeaderHeartbeatMessage {
  type: "leader-heartbeat";
  leaderTabId: string;
  term: number;
  sentAtMs: number;
}

interface LeaderRequestMessage {
  type: "leader-request";
  requesterTabId: string;
}

interface LeaderClaimMessage {
  type: "leader-claim";
  candidateTabId: string;
  term: number;
  sentAtMs: number;
}

type LeaderElectionMessage = LeaderHeartbeatMessage | LeaderRequestMessage | LeaderClaimMessage;

type LeaderChangeListener = (snapshot: LeaderSnapshot) => void;

interface BroadcastChannelLike {
  postMessage(data: unknown): void;
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  removeEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  close(): void;
}

function randomTabId(): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    // Prefer UUID when available to minimize collision risk across tabs/processes.
    return cryptoObj.randomUUID();
  }
  // Fallback for environments lacking randomUUID (still sufficient for local tie-breaks).
  return `tab-${Math.random().toString(36).slice(2, 12)}`;
}

function compareTabIds(a: string, b: string): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

function isMessage(value: unknown): value is LeaderElectionMessage {
  if (typeof value !== "object" || value === null) return false;
  const msg = value as Record<string, unknown>;
  if (msg.type === "leader-heartbeat") {
    return (
      typeof msg.leaderTabId === "string" &&
      typeof msg.term === "number" &&
      typeof msg.sentAtMs === "number"
    );
  }
  if (msg.type === "leader-request") {
    return typeof msg.requesterTabId === "string";
  }
  if (msg.type === "leader-claim") {
    return (
      typeof msg.candidateTabId === "string" &&
      typeof msg.term === "number" &&
      typeof msg.sentAtMs === "number"
    );
  }
  return false;
}

function resolveBroadcastChannelCtor(): (new (name: string) => BroadcastChannelLike) | null {
  const ctor = (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;
  if (typeof ctor !== "function") return null;
  return ctor as new (name: string) => BroadcastChannelLike;
}

export class TabLeaderElection {
  private readonly tabId: string;
  private readonly heartbeatMs: number;
  private readonly leaseMs: number;
  private readonly initialElectionDelayMs: number;
  private readonly now: () => number;
  private readonly channelName: string;

  private started = false;
  private channel: BroadcastChannelLike | null = null;
  private role: LeaderRole = "follower";
  private term = 0;
  private leaderTabId: string | null = null;
  private lastLeaderSeenAtMs = 0;

  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private leaseDeadlineTimer: ReturnType<typeof setTimeout> | null = null;
  private initialElectionTimer: ReturnType<typeof setTimeout> | null = null;

  private readonly listeners = new Set<LeaderChangeListener>();
  private readyResolve: ((snapshot: LeaderSnapshot) => void) | null = null;
  private readyReject: ((reason?: unknown) => void) | null = null;
  private readyPromise: Promise<LeaderSnapshot>;
  private readySettled = false;

  private readonly onMessage = (event: MessageEvent): void => {
    this.handleIncomingMessage(event.data);
  };

  constructor(options: TabLeaderElectionOptions) {
    this.tabId = options.tabId ?? randomTabId();
    this.heartbeatMs = Math.max(100, options.heartbeatMs ?? 1000);
    this.leaseMs = Math.max(this.heartbeatMs * 2, options.leaseMs ?? 5000);
    this.initialElectionDelayMs = Math.max(
      this.heartbeatMs,
      options.initialElectionDelayMs ?? this.heartbeatMs,
    );
    this.now = options.now ?? (() => Date.now());
    this.channelName = `jazz-leader:${options.appId}:${options.dbName}`;

    this.readyPromise = new Promise<LeaderSnapshot>((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
    });
  }

  start(): void {
    if (this.started) return;
    this.started = true;

    const ChannelCtor = resolveBroadcastChannelCtor();
    if (!ChannelCtor) {
      this.promoteToLeader(this.term + 1);
      return;
    }

    this.channel = new ChannelCtor(this.channelName);
    this.channel.addEventListener("message", this.onMessage);

    this.postMessage({
      type: "leader-request",
      requesterTabId: this.tabId,
    });

    // Startup liveness guard:
    // if request/heartbeat messages are dropped or delayed, this tab must not wait forever.
    this.initialElectionTimer = setTimeout(() => {
      if (!this.leaderTabId) {
        this.promoteToLeader(this.term + 1);
      }
    }, this.initialElectionDelayMs);
  }

  stop(): void {
    if (!this.started) return;
    this.started = false;

    if (this.initialElectionTimer) {
      clearTimeout(this.initialElectionTimer);
      this.initialElectionTimer = null;
    }
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.clearLeaseDeadlineTimer();
    if (this.channel) {
      this.channel.removeEventListener("message", this.onMessage);
      this.channel.close();
      this.channel = null;
    }

    if (!this.readySettled && this.readyReject) {
      this.readyReject(new Error("Leader election stopped before initial leader was chosen"));
      this.readyReject = null;
      this.readyResolve = null;
      this.readySettled = true;
    }
  }

  onChange(listener: LeaderChangeListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  snapshot(): LeaderSnapshot {
    return {
      role: this.role,
      tabId: this.tabId,
      leaderTabId: this.leaderTabId,
      term: this.term,
    };
  }

  isLeader(): boolean {
    return this.role === "leader";
  }

  async waitForInitialLeader(timeoutMs = 2000): Promise<LeaderSnapshot> {
    if (this.readySettled) {
      return this.snapshot();
    }

    return await Promise.race([
      this.readyPromise,
      new Promise<LeaderSnapshot>((_resolve, reject) => {
        setTimeout(() => reject(new Error("Leader election timeout")), timeoutMs);
      }),
    ]);
  }

  private handleIncomingMessage(raw: unknown): void {
    if (!isMessage(raw)) return;

    switch (raw.type) {
      case "leader-request":
        if (this.role === "leader") {
          this.sendHeartbeat();
        }
        return;
      case "leader-heartbeat":
        this.handleLeaderHeartbeat(raw);
        return;
      case "leader-claim":
        this.handleLeaderClaim(raw);
        return;
    }
  }

  private handleLeaderHeartbeat(message: LeaderHeartbeatMessage): void {
    const shouldAdopt =
      message.term > this.term ||
      (message.term === this.term &&
        (this.leaderTabId === null ||
          message.leaderTabId === this.leaderTabId ||
          compareTabIds(message.leaderTabId, this.leaderTabId) > 0));

    if (!shouldAdopt) {
      return;
    }

    this.setLeader(message.leaderTabId, message.term);
    this.lastLeaderSeenAtMs = this.now();
    this.scheduleLeaseDeadlineCheck();
  }

  private handleLeaderClaim(message: LeaderClaimMessage): void {
    const shouldAdopt =
      message.term > this.term ||
      (message.term === this.term &&
        (this.leaderTabId === null || compareTabIds(message.candidateTabId, this.leaderTabId) > 0));

    if (!shouldAdopt) {
      return;
    }

    this.setLeader(message.candidateTabId, message.term);
    this.lastLeaderSeenAtMs = this.now();
    this.scheduleLeaseDeadlineCheck();
  }

  private promoteToLeader(nextTerm: number): void {
    const electedTerm = Math.max(this.term + 1, nextTerm);
    this.setLeader(this.tabId, electedTerm);
    this.lastLeaderSeenAtMs = this.now();

    this.postMessage({
      type: "leader-claim",
      candidateTabId: this.tabId,
      term: electedTerm,
      sentAtMs: this.now(),
    });
    this.sendHeartbeat();
  }

  private setLeader(leaderTabId: string, term: number): void {
    const prevLeader = this.leaderTabId;
    const prevRole = this.role;
    const prevTerm = this.term;
    const nextRole: LeaderRole = leaderTabId === this.tabId ? "leader" : "follower";
    this.term = term;
    this.leaderTabId = leaderTabId;
    this.role = nextRole;

    if (this.role === "leader") {
      this.ensureHeartbeatTimer();
      this.clearLeaseDeadlineTimer();
    } else {
      this.clearHeartbeatTimer();
      this.scheduleLeaseDeadlineCheck();
    }

    this.resolveReadyIfNeeded();

    const changed = prevLeader !== leaderTabId || prevRole !== nextRole || prevTerm !== this.term;
    if (changed) {
      this.emitChange();
    }
  }

  private ensureHeartbeatTimer(): void {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      this.sendHeartbeat();
    }, this.heartbeatMs);
  }

  private clearHeartbeatTimer(): void {
    if (!this.heartbeatTimer) return;
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }

  private scheduleLeaseDeadlineCheck(): void {
    if (!this.started || this.role === "leader") {
      this.clearLeaseDeadlineTimer();
      return;
    }

    const referenceTime =
      this.lastLeaderSeenAtMs > 0 ? this.lastLeaderSeenAtMs : this.now() - this.heartbeatMs;
    const deadlineMs = referenceTime + this.leaseMs;
    const delayMs = Math.max(0, deadlineMs - this.now());

    this.clearLeaseDeadlineTimer();
    this.leaseDeadlineTimer = setTimeout(() => {
      this.leaseDeadlineTimer = null;
      this.onLeaseDeadline();
    }, delayMs);
  }

  private clearLeaseDeadlineTimer(): void {
    if (!this.leaseDeadlineTimer) return;
    clearTimeout(this.leaseDeadlineTimer);
    this.leaseDeadlineTimer = null;
  }

  private onLeaseDeadline(): void {
    if (!this.started || this.role === "leader") return;
    if (!this.leaderTabId) {
      this.promoteToLeader(this.term + 1);
      return;
    }

    const elapsed = this.now() - this.lastLeaderSeenAtMs;
    if (elapsed >= this.leaseMs) {
      this.promoteToLeader(this.term + 1);
      return;
    }

    this.scheduleLeaseDeadlineCheck();
  }

  private sendHeartbeat(): void {
    if (!this.started || this.role !== "leader") return;
    this.postMessage({
      type: "leader-heartbeat",
      leaderTabId: this.tabId,
      term: this.term,
      sentAtMs: this.now(),
    });
  }

  private postMessage(message: LeaderElectionMessage): void {
    this.channel?.postMessage(message);
  }

  private emitChange(): void {
    const snapshot = this.snapshot();
    for (const listener of this.listeners) {
      listener(snapshot);
    }
  }

  private resolveReadyIfNeeded(): void {
    if (this.readySettled || !this.leaderTabId || !this.readyResolve) return;
    this.readySettled = true;
    this.readyResolve(this.snapshot());
    this.readyResolve = null;
    this.readyReject = null;
  }
}
