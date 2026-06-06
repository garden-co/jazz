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
  schemaFingerprint: string | null;
  port: MessagePort;
  lastPongAt: number;
};

type LeaderState = {
  tabId: string;
  leadershipId: number;
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
  "tabId" | "leadershipId" | "tabLockName" | "workerLockName" | "compatibilityLockName"
>;

type ResetState = {
  requestId: string;
  participants: Set<string>;
  preparedTabs: Set<string>;
  errors: string[];
  previousLeader: ClearedLeaderState | null;
  promotedLeadershipId: number | null;
  phase: "preparing" | "promoting" | "reconnecting";
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const brokerInstanceId = createBrokerId("broker");
const tabs = new Map<string, TabState>();
let namespace: {
  appId: string;
  dbName: string;
  fingerprint: string;
  forceTakeoverTimeoutMs: number;
  brokerPingIntervalMs: number;
  brokerPongTimeoutMs: number;
  schemaFingerprint: string | null;
} | null = null;
let leader: LeaderState | null = null;
let currentLeadershipId = 0;
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
      schemaFingerprint: null,
    };
  }

  if (
    namespace.appId !== message.appId ||
    namespace.dbName !== message.dbName ||
    namespace.fingerprint !== message.fingerprint
  ) {
    post(port, {
      type: "unsupported",
      brokerInstanceId,
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
    schemaFingerprint: null,
    visibility: message.visibility,
    lastVisibleAt: message.visibility === "visible" ? now : 0,
    port,
    lastPongAt: now,
  });

  post(port, { type: "broker-hello", brokerInstanceId });
  startBrokerPingTimer();
  if (resetState) {
    addTabToActiveReset(message.tabId);
    return message.tabId;
  }
  if (leader?.ready) {
    post(port, {
      type: "leader-ready",
      brokerInstanceId,
      leaderTabId: leader.tabId,
      leadershipId: leader.leadershipId,
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
      if (!leader || leader.tabId !== tabId || leader.leadershipId !== message.leadershipId) return;
      leader.ready = true;
      leader.tabLockName = message.tabLockName;
      leader.workerLockName = message.workerLockName;
      leader.compatibilityLockName = message.compatibilityLockName ?? null;
      announceLeaderReady(leader);
      startLeaderLockMonitors(leader);
      if (resetState?.promotedLeadershipId === message.leadershipId) {
        resetState.phase = "reconnecting";
        assignFollowerPorts(leader);
        finishStorageResetIfReconnected(resetState);
        return;
      }
      assignFollowerPorts(leader);
      return;
    case "follower-port-attached":
      if (!leader || leader.tabId !== tabId || leader.leadershipId !== message.leadershipId) return;
      markFollowerPortAttached(message.followerTabId, message.leadershipId);
      return;
    case "schema-ready":
      handleSchemaReady(tabId, message.schemaFingerprint);
      return;
    case "leader-failed":
      if (resetState?.promotedLeadershipId === message.leadershipId) {
        resetState.errors.push(message.reason);
        tabs.delete(tabId);
        removeTabFromActiveReset(tabId);
        leader = null;
        resetState.promotedLeadershipId = null;
        void promoteResetLeader(resetState);
        return;
      }
      if (leader?.tabId === tabId && leader.leadershipId === message.leadershipId) {
        const failedTab = tabs.get(tabId);
        const cleared = clearLeader(message.leadershipId, "leader-failed", {
          demoteLeader: false,
          removeLeaderTab: true,
        });
        if (failedTab) {
          post(failedTab.port, {
            type: "unsupported",
            brokerInstanceId,
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
      if (leader?.tabId === tabId) {
        const leadershipId = leader.leadershipId;
        const activeReset = resetState;
        const cleared = clearLeader(leader.leadershipId, "leader-shutdown", {
          demoteLeader: false,
          removeLeaderTab: false,
        });
        removeTabFromActiveReset(tabId);
        if (
          activeReset &&
          activeReset.promotedLeadershipId === leadershipId &&
          activeReset.phase !== "preparing"
        ) {
          activeReset.promotedLeadershipId = null;
          void promoteResetLeader(activeReset);
          resetIfIdle();
          return;
        }
        scheduleReplacementElection(cleared);
      } else {
        removeTabFromActiveReset(tabId);
      }
      resetIfIdle();
      return;
    case "broker-pong":
      if (message.brokerInstanceId !== brokerInstanceId) return;
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
  if (leader?.ready || tabs.size === 0) return;
  if (leader && !namespace?.schemaFingerprint) return;

  const candidate = selectLeaderCandidate(eligibleLeaderCandidates());
  if (!candidate) return;

  if (leader) {
    const currentLeaderTab = tabs.get(leader.tabId);
    if (currentLeaderTab?.schemaFingerprint === namespace?.schemaFingerprint) {
      return;
    }
    if (candidate.tabId === leader.tabId) {
      return;
    }
    clearLeader(leader.leadershipId, "leader-not-ready-for-schema", {
      demoteLeader: true,
      removeLeaderTab: false,
    });
  }

  const tab = tabs.get(candidate.tabId);
  if (!tab) return;

  currentLeadershipId += 1;
  leader = {
    tabId: tab.tabId,
    leadershipId: currentLeadershipId,
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
    brokerInstanceId,
    leadershipId: currentLeadershipId,
  });
}

function resetIfIdle(): void {
  if (tabs.size > 0) return;
  namespace = null;
  leader = null;
  currentLeadershipId = 0;
  pendingFollowerAttachments.clear();
  attachedFollowerPorts.clear();
  resetState = null;
  replacementElectionInFlight = false;
  stopBrokerPingTimer();
}

function handleSchemaReady(tabId: string, schemaFingerprint: string): void {
  if (!namespace) return;
  const tab = tabs.get(tabId);
  if (!tab) return;

  if (!namespace.schemaFingerprint) {
    namespace.schemaFingerprint = schemaFingerprint;
  }

  if (namespace.schemaFingerprint !== schemaFingerprint) {
    rejectTabForSchemaMismatch(tabId, schemaFingerprint);
    return;
  }

  tab.schemaFingerprint = schemaFingerprint;
  if (leader?.ready) {
    assignFollowerPorts(leader);
    return;
  }
  electIfNeeded();
}

function rejectTabForSchemaMismatch(tabId: string, schemaFingerprint: string): void {
  const tab = tabs.get(tabId);
  if (!tab) return;

  post(tab.port, {
    type: "unsupported",
    brokerInstanceId,
    reason: "incompatible persistent browser schema",
  });
  tab.port.close();
  tabs.delete(tabId);

  if (leader?.tabId === tabId) {
    const cleared = clearLeader(leader.leadershipId, `schema mismatch: ${schemaFingerprint}`, {
      demoteLeader: false,
      removeLeaderTab: false,
    });
    scheduleReplacementElection(cleared);
  }
  resetIfIdle();
}

function announceLeaderReady(nextLeader: LeaderState): void {
  for (const tab of tabs.values()) {
    post(tab.port, {
      type: "leader-ready",
      brokerInstanceId,
      leaderTabId: nextLeader.tabId,
      leadershipId: nextLeader.leadershipId,
    });
  }
}

function startLeaderLockMonitors(nextLeader: LeaderState): void {
  cancelLeaderMonitors(nextLeader);
  if (!nextLeader.tabLockName || !nextLeader.workerLockName) return;

  nextLeader.tabLockMonitor = monitorWebLockRelease(nextLeader.tabLockName, {
    onGranted: () => {
      handleLeaderLockReleased(nextLeader.leadershipId, "tab-lock-released");
    },
    onError: (error) => {
      handleLeaderLockReleased(nextLeader.leadershipId, stringifyError(error));
    },
  });
  nextLeader.workerLockMonitor = monitorWebLockRelease(nextLeader.workerLockName, {
    onGranted: () => {
      handleLeaderLockReleased(nextLeader.leadershipId, "worker-lock-released");
    },
    onError: (error) => {
      handleLeaderLockReleased(nextLeader.leadershipId, stringifyError(error));
    },
  });
  if (nextLeader.compatibilityLockName) {
    nextLeader.compatibilityLockMonitor = monitorWebLockRelease(nextLeader.compatibilityLockName, {
      onGranted: () => {
        handleLeaderLockReleased(nextLeader.leadershipId, "compatibility-lock-released");
      },
      onError: (error) => {
        handleLeaderLockReleased(nextLeader.leadershipId, stringifyError(error));
      },
    });
  }
}

function handleLeaderLockReleased(leadershipId: number, reason: string): void {
  if (!leader || leader.leadershipId !== leadershipId) return;
  const cleared = clearLeader(leadershipId, reason, { demoteLeader: true, removeLeaderTab: true });
  scheduleReplacementElection(cleared);
}

function clearLeader(
  leadershipId: number,
  reason: string,
  options: { demoteLeader: boolean; removeLeaderTab: boolean },
): ClearedLeaderState | null {
  const current = leader;
  if (!current || current.leadershipId !== leadershipId) return null;
  cancelLeaderMonitors(current);
  pendingFollowerAttachments.clear();
  attachedFollowerPorts.clear();

  const leaderTab = tabs.get(current.tabId);
  if (options.demoteLeader && leaderTab) {
    post(leaderTab.port, {
      type: "demote",
      brokerInstanceId,
      leadershipId,
    });
  }

  for (const tab of tabs.values()) {
    if (tab.tabId === current.tabId) continue;
    post(tab.port, {
      type: "close-follower-port",
      brokerInstanceId,
      leadershipId,
    });
  }

  if (options.removeLeaderTab) {
    tabs.delete(current.tabId);
  }
  leader = null;
  void reason;
  return {
    tabId: current.tabId,
    leadershipId: current.leadershipId,
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
    ? clearLeader(leader.leadershipId, "storage-reset", {
        demoteLeader: false,
        removeLeaderTab: false,
      })
    : null;

  const leadershipId = previousLeader?.leadershipId ?? currentLeadershipId;
  resetState = {
    requestId,
    participants: new Set(tabs.keys()),
    preparedTabs: new Set(),
    errors: [],
    previousLeader,
    promotedLeadershipId: null,
    phase: "preparing",
  };

  for (const tab of tabs.values()) {
    post(tab.port, {
      type: "storage-reset-begin",
      brokerInstanceId,
      requestId,
      leadershipId,
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
    brokerInstanceId,
    requestId: activeReset.requestId,
    leadershipId: activeReset.previousLeader?.leadershipId ?? currentLeadershipId,
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
  const candidate = selectLeaderCandidate(eligibleLeaderCandidates());
  if (!candidate) {
    finishStorageReset(activeReset, false, "No connected tab is available to reset storage");
    return;
  }

  const tab = tabs.get(candidate.tabId);
  if (!tab) {
    finishStorageReset(activeReset, false, "No connected tab is available to reset storage");
    return;
  }

  currentLeadershipId += 1;
  activeReset.promotedLeadershipId = currentLeadershipId;
  leader = {
    tabId: tab.tabId,
    leadershipId: currentLeadershipId,
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
    brokerInstanceId,
    leadershipId: currentLeadershipId,
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
      brokerInstanceId,
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
  if (!leader || !leader.ready || activeReset.promotedLeadershipId !== leader.leadershipId) return;

  for (const tabId of activeReset.participants) {
    const tab = tabs.get(tabId);
    if (!tab || !shouldAssignFollowerPort(tab, leader)) continue;
    if (!attachedFollowerPorts.has(followerAttachmentKey(tabId, leader.leadershipId))) {
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
      brokerInstanceId,
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
    const leadershipId = leader.leadershipId;
    const activeReset = resetState;
    const cleared = clearLeader(leader.leadershipId, reason, {
      demoteLeader: false,
      removeLeaderTab: false,
    });
    removeTabFromActiveReset(tabId);
    if (
      activeReset &&
      activeReset.promotedLeadershipId === leadershipId &&
      activeReset.phase !== "preparing"
    ) {
      activeReset.promotedLeadershipId = null;
      void promoteResetLeader(activeReset);
      resetIfIdle();
      return;
    }
    scheduleReplacementElection(cleared);
  } else {
    removeTabFromActiveReset(tabId);
  }
  resetIfIdle();
}

async function waitForPreviousLeaderLocks(
  previousLeader: ClearedLeaderState | null,
): Promise<void> {
  if (!previousLeader?.tabLockName || !previousLeader.workerLockName) {
    return;
  }

  const takeoverLockNames = [previousLeader.tabLockName, previousLeader.workerLockName].filter(
    (lockName): lockName is string => lockName !== null,
  );

  if (await acquireAndReleaseLocks(takeoverLockNames)) {
    return;
  }

  await sleep(namespace?.forceTakeoverTimeoutMs ?? DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS);
  for (const lockName of takeoverLockNames) {
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
    if (!shouldAssignFollowerPort(follower, nextLeader)) continue;
    const key = followerAttachmentKey(follower.tabId, nextLeader.leadershipId);
    if (pendingFollowerAttachments.has(key)) continue;
    if (attachedFollowerPorts.has(key)) continue;

    const channel = new MessageChannel();
    pendingFollowerAttachments.add(key);
    post(
      leaderTab.port,
      {
        type: "attach-follower-port",
        brokerInstanceId,
        followerTabId: follower.tabId,
        leadershipId: nextLeader.leadershipId,
        port: channel.port1,
      },
      [channel.port1],
    );
    post(
      follower.port,
      {
        type: "use-follower-port",
        brokerInstanceId,
        leaderTabId: nextLeader.tabId,
        leadershipId: nextLeader.leadershipId,
        port: channel.port2,
      },
      [channel.port2],
    );
  }
}

function eligibleLeaderCandidates(): BrowserBrokerCandidate[] {
  const schemaFingerprint = namespace?.schemaFingerprint;
  if (!schemaFingerprint) {
    return [...tabs.values()];
  }
  return [...tabs.values()].filter((tab) => tab.schemaFingerprint === schemaFingerprint);
}

function shouldAssignFollowerPort(tab: TabState, nextLeader: LeaderState): boolean {
  if (tab.tabId === nextLeader.tabId) return false;
  return !namespace?.schemaFingerprint || tab.schemaFingerprint === namespace.schemaFingerprint;
}

function removeTabFromActiveReset(tabId: string): void {
  const activeReset = resetState;
  if (!activeReset) return;

  activeReset.participants.delete(tabId);
  activeReset.preparedTabs.delete(tabId);
  continueStorageResetIfReady(activeReset);
}

function markFollowerPortAttached(followerTabId: string, leadershipId: number): void {
  const key = followerAttachmentKey(followerTabId, leadershipId);
  if (!pendingFollowerAttachments.has(key)) return;
  pendingFollowerAttachments.delete(key);

  if (!leader || leader.leadershipId !== leadershipId) return;
  const follower = tabs.get(followerTabId);
  if (!follower) return;
  attachedFollowerPorts.add(key);

  post(follower.port, {
    type: "follower-ready",
    brokerInstanceId,
    leaderTabId: leader.tabId,
    leadershipId,
  });
  if (resetState?.phase === "reconnecting" && resetState.promotedLeadershipId === leadershipId) {
    finishStorageResetIfReconnected(resetState);
  }
}

function followerAttachmentKey(followerTabId: string, leadershipId: number): string {
  return `${leadershipId}:${followerTabId}`;
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
