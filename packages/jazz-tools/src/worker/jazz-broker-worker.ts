import {
  selectLeaderCandidate,
  type BrowserBrokerCandidate,
  type BrowserBrokerControlMessage,
  type BrowserBrokerTabMessage,
  type BrowserBrokerVisibility,
} from "../runtime/browser-broker-protocol.js";
import {
  monitorWebLockRelease,
  stealAndReleaseWebLock,
  tryAcquireWebLock,
  type WebLockMonitor,
} from "../runtime/leader-lock.js";

const DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS = 1_000;
const DEFAULT_BROKER_PING_INTERVAL_MS = 1_000;
const DEFAULT_BROKER_PONG_TIMEOUT_MS = 3_000;

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
};

type TabState = BrowserBrokerCandidate & {
  appId: string;
  dbName: string;
  fingerprint: string;
  port: MessagePort;
  lastPongAt: number;
};

type LeaderState = {
  tabId: string;
  term: number;
  ready: boolean;
  tabLockName: string | null;
  workerLockName: string | null;
  compatibilityLockName: string | null;
  tabLockMonitor: WebLockMonitor | null;
  workerLockMonitor: WebLockMonitor | null;
  compatibilityLockMonitor: WebLockMonitor | null;
};

type ClearedLeaderState = Pick<
  LeaderState,
  "tabId" | "term" | "tabLockName" | "workerLockName" | "compatibilityLockName"
>;

type ResetState = {
  requestId: string;
  participants: Set<string>;
  preparedTabs: Set<string>;
  errors: string[];
  previousLeader: ClearedLeaderState | null;
  promotedTerm: number | null;
  phase: "preparing" | "promoting" | "reconnecting";
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const brokerEpoch = createBrokerId("epoch");
const tabs = new Map<string, TabState>();
let namespace: {
  appId: string;
  dbName: string;
  fingerprint: string;
  forceTakeoverTimeoutMs: number;
  brokerPingIntervalMs: number;
  brokerPongTimeoutMs: number;
} | null = null;
let leader: LeaderState | null = null;
let currentTerm = 0;
const pendingFollowerAttachments = new Set<string>();
const attachedFollowerPorts = new Set<string>();
let replacementElectionInFlight = false;
let brokerPingTimer: ReturnType<typeof setTimeout> | null = null;
let resetState: ResetState | null = null;

workerGlobal.onconnect = (event) => {
  const port = event.ports[0];
  if (!port) return;

  let tabId: string | null = null;

  port.addEventListener("message", (messageEvent) => {
    const message = messageEvent.data as BrowserBrokerTabMessage;
    if (!message || typeof message !== "object") return;

    if (message.type === "hello") {
      tabId = handleHello(port, message);
      return;
    }

    if (!tabId) return;
    handleTabMessage(tabId, message);
  });
  port.start();
};

function handleHello(port: MessagePort, message: BrowserBrokerTabMessage): string | null {
  if (message.type !== "hello") return null;

  if (!namespace) {
    namespace = {
      appId: message.appId,
      dbName: message.dbName,
      fingerprint: message.fingerprint,
      forceTakeoverTimeoutMs: normalizeForceTakeoverTimeout(message.forceTakeoverTimeoutMs),
      brokerPingIntervalMs: normalizePositiveTimeout(
        message.brokerPingIntervalMs,
        DEFAULT_BROKER_PING_INTERVAL_MS,
      ),
      brokerPongTimeoutMs: normalizePositiveTimeout(
        message.brokerPongTimeoutMs,
        DEFAULT_BROKER_PONG_TIMEOUT_MS,
      ),
    };
  }

  if (
    namespace.appId !== message.appId ||
    namespace.dbName !== message.dbName ||
    namespace.fingerprint !== message.fingerprint
  ) {
    post(port, {
      type: "unsupported",
      brokerEpoch,
      reason: "incompatible persistent browser configuration",
    });
    port.close();
    return null;
  }

  const now = Date.now();
  tabs.set(message.tabId, {
    tabId: message.tabId,
    appId: message.appId,
    dbName: message.dbName,
    fingerprint: message.fingerprint,
    visibility: message.visibility,
    lastVisibleAt: message.visibility === "visible" ? now : 0,
    port,
    lastPongAt: now,
  });

  post(port, { type: "broker-hello", brokerEpoch });
  startBrokerPingTimer();
  if (resetState) {
    addTabToActiveReset(message.tabId);
    return message.tabId;
  }
  if (leader?.ready) {
    post(port, {
      type: "leader-ready",
      brokerEpoch,
      leaderTabId: leader.tabId,
      term: leader.term,
    });
    assignFollowerPorts(leader);
  } else {
    electIfNeeded();
  }

  return message.tabId;
}

function handleTabMessage(tabId: string, message: BrowserBrokerTabMessage): void {
  switch (message.type) {
    case "visibility":
      updateVisibility(tabId, message.visibility);
      evictStaleTabs();
      return;
    case "leader-ready":
      if (!leader || leader.tabId !== tabId || leader.term !== message.term) return;
      leader.ready = true;
      leader.tabLockName = message.tabLockName;
      leader.workerLockName = message.workerLockName;
      leader.compatibilityLockName = message.compatibilityLockName ?? null;
      announceLeaderReady(leader);
      startLeaderLockMonitors(leader);
      if (resetState?.promotedTerm === message.term) {
        resetState.phase = "reconnecting";
        assignFollowerPorts(leader);
        finishStorageResetIfReconnected(resetState);
        return;
      }
      assignFollowerPorts(leader);
      return;
    case "follower-port-attached":
      if (!leader || leader.tabId !== tabId || leader.term !== message.term) return;
      markFollowerPortAttached(message.followerTabId, message.term);
      return;
    case "leader-failed":
      if (resetState?.promotedTerm === message.term) {
        resetState.errors.push(message.reason);
        tabs.delete(tabId);
        leader = null;
        void promoteResetLeader(resetState);
        return;
      }
      if (leader?.tabId === tabId && leader.term === message.term) {
        const failedTab = tabs.get(tabId);
        const cleared = clearLeader(message.term, "leader-failed", {
          demoteLeader: false,
          removeLeaderTab: true,
        });
        if (failedTab) {
          post(failedTab.port, {
            type: "unsupported",
            brokerEpoch,
            reason: message.reason,
          });
        }
        scheduleReplacementElection(cleared);
      }
      return;
    case "storage-reset-request":
      startStorageReset(message.requestId);
      return;
    case "storage-reset-ready":
      handleStorageResetReady(tabId, message.requestId, message.success, message.errorMessage);
      return;
    case "shutdown":
      tabs.delete(tabId);
      if (resetState) {
        resetState.participants.delete(tabId);
        resetState.preparedTabs.delete(tabId);
        continueStorageResetIfReady(resetState);
      }
      if (leader?.tabId === tabId) {
        const cleared = clearLeader(leader.term, "leader-shutdown", {
          demoteLeader: false,
          removeLeaderTab: false,
        });
        scheduleReplacementElection(cleared);
      }
      resetIfIdle();
      return;
    case "broker-pong":
      if (message.brokerEpoch !== brokerEpoch) return;
      tabs.get(tabId)!.lastPongAt = Date.now();
      evictStaleTabs();
      return;
  }
}

function updateVisibility(tabId: string, visibility: BrowserBrokerVisibility): void {
  const tab = tabs.get(tabId);
  if (!tab) return;

  tab.visibility = visibility;
  if (visibility === "visible") {
    tab.lastVisibleAt = Date.now();
  }
}

function electIfNeeded(): void {
  if (resetState) return;
  if (leader || tabs.size === 0) return;

  const candidate = selectLeaderCandidate([...tabs.values()]);
  if (!candidate) return;

  const tab = tabs.get(candidate.tabId);
  if (!tab) return;

  currentTerm += 1;
  leader = {
    tabId: tab.tabId,
    term: currentTerm,
    ready: false,
    tabLockName: null,
    workerLockName: null,
    compatibilityLockName: null,
    tabLockMonitor: null,
    workerLockMonitor: null,
    compatibilityLockMonitor: null,
  };

  post(tab.port, {
    type: "become-leader",
    brokerEpoch,
    term: currentTerm,
  });
}

function resetIfIdle(): void {
  if (tabs.size > 0) return;
  namespace = null;
  leader = null;
  currentTerm = 0;
  pendingFollowerAttachments.clear();
  attachedFollowerPorts.clear();
  resetState = null;
  replacementElectionInFlight = false;
  stopBrokerPingTimer();
}

function announceLeaderReady(nextLeader: LeaderState): void {
  for (const tab of tabs.values()) {
    post(tab.port, {
      type: "leader-ready",
      brokerEpoch,
      leaderTabId: nextLeader.tabId,
      term: nextLeader.term,
    });
  }
}

function startLeaderLockMonitors(nextLeader: LeaderState): void {
  cancelLeaderMonitors(nextLeader);
  if (!nextLeader.tabLockName || !nextLeader.workerLockName) return;

  nextLeader.tabLockMonitor = monitorWebLockRelease(nextLeader.tabLockName, {
    onGranted: () => {
      handleLeaderLockReleased(nextLeader.term, "tab-lock-released");
    },
    onError: (error) => {
      handleLeaderLockReleased(nextLeader.term, stringifyError(error));
    },
  });
  nextLeader.workerLockMonitor = monitorWebLockRelease(nextLeader.workerLockName, {
    onGranted: () => {
      handleLeaderLockReleased(nextLeader.term, "worker-lock-released");
    },
    onError: (error) => {
      handleLeaderLockReleased(nextLeader.term, stringifyError(error));
    },
  });
  if (nextLeader.compatibilityLockName) {
    nextLeader.compatibilityLockMonitor = monitorWebLockRelease(nextLeader.compatibilityLockName, {
      onGranted: () => {
        handleLeaderLockReleased(nextLeader.term, "compatibility-lock-released");
      },
      onError: (error) => {
        handleLeaderLockReleased(nextLeader.term, stringifyError(error));
      },
    });
  }
}

function handleLeaderLockReleased(term: number, reason: string): void {
  if (!leader || leader.term !== term) return;
  const cleared = clearLeader(term, reason, { demoteLeader: true, removeLeaderTab: true });
  scheduleReplacementElection(cleared);
}

function clearLeader(
  term: number,
  reason: string,
  options: { demoteLeader: boolean; removeLeaderTab: boolean },
): ClearedLeaderState | null {
  const current = leader;
  if (!current || current.term !== term) return null;
  cancelLeaderMonitors(current);
  pendingFollowerAttachments.clear();
  attachedFollowerPorts.clear();

  const leaderTab = tabs.get(current.tabId);
  if (options.demoteLeader && leaderTab) {
    post(leaderTab.port, {
      type: "demote",
      brokerEpoch,
      term,
    });
  }

  for (const tab of tabs.values()) {
    if (tab.tabId === current.tabId) continue;
    post(tab.port, {
      type: "close-follower-port",
      brokerEpoch,
      term,
    });
  }

  if (options.removeLeaderTab) {
    tabs.delete(current.tabId);
  }
  leader = null;
  void reason;
  return {
    tabId: current.tabId,
    term: current.term,
    tabLockName: current.tabLockName,
    workerLockName: current.workerLockName,
    compatibilityLockName: current.compatibilityLockName,
  };
}

function cancelLeaderMonitors(current: LeaderState): void {
  current.tabLockMonitor?.cancel();
  current.tabLockMonitor = null;
  current.workerLockMonitor?.cancel();
  current.workerLockMonitor = null;
  current.compatibilityLockMonitor?.cancel();
  current.compatibilityLockMonitor = null;
}

function startStorageReset(requestId: string): void {
  if (resetState) {
    return;
  }

  const previousLeader = leader
    ? clearLeader(leader.term, "storage-reset", {
        demoteLeader: false,
        removeLeaderTab: false,
      })
    : null;

  const term = previousLeader?.term ?? currentTerm;
  resetState = {
    requestId,
    participants: new Set(tabs.keys()),
    preparedTabs: new Set(),
    errors: [],
    previousLeader,
    promotedTerm: null,
    phase: "preparing",
  };

  for (const tab of tabs.values()) {
    post(tab.port, {
      type: "storage-reset-begin",
      brokerEpoch,
      requestId,
      term,
    });
  }

  continueStorageResetIfReady(resetState);
}

function addTabToActiveReset(tabId: string): void {
  const activeReset = resetState;
  const tab = tabs.get(tabId);
  if (!activeReset || !tab) return;
  if (activeReset.phase !== "preparing") return;

  activeReset.participants.add(tabId);
  post(tab.port, {
    type: "storage-reset-begin",
    brokerEpoch,
    requestId: activeReset.requestId,
    term: activeReset.previousLeader?.term ?? currentTerm,
  });
}

function handleStorageResetReady(
  tabId: string,
  requestId: string,
  success: boolean,
  errorMessage: string | undefined,
): void {
  const activeReset = resetState;
  if (!activeReset || activeReset.requestId !== requestId || activeReset.phase !== "preparing") {
    return;
  }

  if (!success) {
    activeReset.errors.push(errorMessage ?? `Tab ${tabId} failed to prepare storage reset`);
  }
  activeReset.preparedTabs.add(tabId);
  continueStorageResetIfReady(activeReset);
}

function continueStorageResetIfReady(activeReset: ResetState): void {
  if (resetState !== activeReset || activeReset.phase !== "preparing") return;
  if (activeReset.preparedTabs.size < activeReset.participants.size) return;

  activeReset.phase = "promoting";
  void (async () => {
    await waitForPreviousLeaderLocks(activeReset.previousLeader);
    if (activeReset.errors.length > 0) {
      finishStorageReset(activeReset, false, activeReset.errors.join("; "));
      return;
    }
    await promoteResetLeader(activeReset);
  })();
}

async function promoteResetLeader(activeReset: ResetState): Promise<void> {
  if (resetState !== activeReset) return;
  const candidate = selectLeaderCandidate([...tabs.values()]);
  if (!candidate) {
    finishStorageReset(activeReset, false, "No connected tab is available to reset storage");
    return;
  }

  const tab = tabs.get(candidate.tabId);
  if (!tab) {
    finishStorageReset(activeReset, false, "No connected tab is available to reset storage");
    return;
  }

  currentTerm += 1;
  activeReset.promotedTerm = currentTerm;
  leader = {
    tabId: tab.tabId,
    term: currentTerm,
    ready: false,
    tabLockName: null,
    workerLockName: null,
    compatibilityLockName: null,
    tabLockMonitor: null,
    workerLockMonitor: null,
    compatibilityLockMonitor: null,
  };

  post(tab.port, {
    type: "become-leader",
    brokerEpoch,
    term: currentTerm,
    resetRequestId: activeReset.requestId,
  });
}

function finishStorageReset(
  completedReset: ResetState,
  success: boolean,
  errorMessage?: string,
): void {
  if (resetState === completedReset) {
    resetState = null;
  }

  for (const tab of tabs.values()) {
    post(tab.port, {
      type: "storage-reset-finished",
      brokerEpoch,
      requestId: completedReset.requestId,
      success,
      ...(errorMessage ? { errorMessage } : {}),
    });
  }

  if (!success) {
    electIfNeeded();
  }
}

function finishStorageResetIfReconnected(activeReset: ResetState): void {
  if (resetState !== activeReset || activeReset.phase !== "reconnecting") return;
  if (!leader || !leader.ready || activeReset.promotedTerm !== leader.term) return;

  for (const tabId of activeReset.participants) {
    if (tabId === leader.tabId || !tabs.has(tabId)) continue;
    if (!attachedFollowerPorts.has(followerAttachmentKey(tabId, leader.term))) {
      return;
    }
  }

  finishStorageReset(activeReset, true);
}

function scheduleReplacementElection(previousLeader: ClearedLeaderState | null): void {
  if (replacementElectionInFlight) return;
  replacementElectionInFlight = true;

  void (async () => {
    try {
      await waitForPreviousLeaderLocks(previousLeader);
    } finally {
      replacementElectionInFlight = false;
    }
    electIfNeeded();
  })();
}

function startBrokerPingTimer(): void {
  if (brokerPingTimer || !namespace) return;
  sendBrokerPings();
  brokerPingTimer = setTimeout(() => {
    brokerPingTimer = null;
    sendBrokerPings();
    if (namespace && tabs.size > 0) {
      startBrokerPingTimer();
    }
  }, namespace.brokerPingIntervalMs);
}

function stopBrokerPingTimer(): void {
  if (!brokerPingTimer) return;
  clearTimeout(brokerPingTimer);
  brokerPingTimer = null;
}

function sendBrokerPings(): void {
  if (!namespace) return;
  const now = Date.now();

  const tabSnapshot = Array.from(tabs.values());
  for (const tab of tabSnapshot) {
    if (isBrokerPongTimedOut(tab, now)) {
      evictTab(tab.tabId, "missed broker pong");
      continue;
    }
    post(tab.port, {
      type: "broker-ping",
      brokerEpoch,
    });
  }
}

function evictStaleTabs(now = Date.now()): void {
  if (!namespace) return;

  const tabSnapshot = Array.from(tabs.values());
  for (const tab of tabSnapshot) {
    if (isBrokerPongTimedOut(tab, now)) {
      evictTab(tab.tabId, "missed broker pong");
    }
  }
}

function isBrokerPongTimedOut(tab: TabState, now: number): boolean {
  return namespace !== null && now - tab.lastPongAt > namespace.brokerPongTimeoutMs;
}

function evictTab(tabId: string, reason: string): void {
  const tab = tabs.get(tabId);
  if (!tab) return;
  tabs.delete(tabId);
  tab.port.close();

  if (leader?.tabId === tabId) {
    const cleared = clearLeader(leader.term, reason, {
      demoteLeader: false,
      removeLeaderTab: false,
    });
    scheduleReplacementElection(cleared);
  }
  resetIfIdle();
}

async function waitForPreviousLeaderLocks(
  previousLeader: ClearedLeaderState | null,
): Promise<void> {
  if (!previousLeader?.tabLockName || !previousLeader.workerLockName) {
    return;
  }

  const lockNames = [
    previousLeader.tabLockName,
    previousLeader.workerLockName,
    previousLeader.compatibilityLockName,
  ].filter((lockName): lockName is string => lockName !== null);

  if (await acquireAndReleaseLocks(lockNames)) {
    return;
  }

  await sleep(namespace?.forceTakeoverTimeoutMs ?? DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS);
  for (const lockName of lockNames) {
    await stealAndReleaseWebLock(lockName).catch(() => undefined);
  }
}

async function acquireAndReleaseLocks(lockNames: readonly string[]): Promise<boolean> {
  const leases = await Promise.all(lockNames.map((lockName) => tryAcquireWebLock(lockName)));
  for (const lease of leases) {
    lease?.release();
  }
  return leases.every((lease) => lease !== null);
}

function assignFollowerPorts(nextLeader: LeaderState): void {
  if (resetState && resetState.phase !== "reconnecting") return;
  const leaderTab = tabs.get(nextLeader.tabId);
  if (!leaderTab) return;

  for (const follower of tabs.values()) {
    if (follower.tabId === nextLeader.tabId) continue;
    const key = followerAttachmentKey(follower.tabId, nextLeader.term);
    if (pendingFollowerAttachments.has(key)) continue;
    if (attachedFollowerPorts.has(key)) continue;

    const channel = new MessageChannel();
    pendingFollowerAttachments.add(key);
    post(
      leaderTab.port,
      {
        type: "attach-follower-port",
        brokerEpoch,
        followerTabId: follower.tabId,
        term: nextLeader.term,
        port: channel.port1,
      },
      [channel.port1],
    );
    post(
      follower.port,
      {
        type: "use-follower-port",
        brokerEpoch,
        leaderTabId: nextLeader.tabId,
        term: nextLeader.term,
        port: channel.port2,
      },
      [channel.port2],
    );
  }
}

function markFollowerPortAttached(followerTabId: string, term: number): void {
  const key = followerAttachmentKey(followerTabId, term);
  if (!pendingFollowerAttachments.has(key)) return;
  pendingFollowerAttachments.delete(key);

  if (!leader || leader.term !== term) return;
  const follower = tabs.get(followerTabId);
  if (!follower) return;
  attachedFollowerPorts.add(key);

  post(follower.port, {
    type: "follower-ready",
    brokerEpoch,
    leaderTabId: leader.tabId,
    term,
  });
  if (resetState?.phase === "reconnecting" && resetState.promotedTerm === term) {
    finishStorageResetIfReconnected(resetState);
  }
}

function followerAttachmentKey(followerTabId: string, term: number): string {
  return `${term}:${followerTabId}`;
}

function post(
  port: MessagePort,
  message: BrowserBrokerControlMessage,
  transfer?: Transferable[],
): void {
  if (transfer) {
    port.postMessage(message, transfer);
    return;
  }
  port.postMessage(message);
}

function createBrokerId(prefix: string): string {
  const cryptoObj = globalThis.crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return `${prefix}-${cryptoObj.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function stringifyError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function normalizeForceTakeoverTimeout(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS;
  }
  return Math.max(0, Math.floor(value));
}

function normalizePositiveTimeout(value: unknown, fallback: number): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.max(1, Math.floor(value));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
