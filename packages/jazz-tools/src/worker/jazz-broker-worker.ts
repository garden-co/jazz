import {
  createRandomId,
  DEFAULT_BROKER_PING_INTERVAL_MS,
  DEFAULT_BROKER_PONG_TIMEOUT_MS,
  normalizePositiveTimeout,
  selectLeaderCandidate,
  stringifyError,
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
const LEADER_FAILURE_RETRY_BACKOFF_MS = 1_000;
const INITIAL_FOLLOWER_ATTACHMENT_TIMEOUT_MS = 1_000;
const MAX_FOLLOWER_ATTACHMENT_TIMEOUT_MS = 30_000;
const COMPLETED_STORAGE_RESET_OUTCOME_TTL_MS = 30_000;
const MAX_COMPLETED_STORAGE_RESET_OUTCOMES = 100;

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
  tabLockMonitor: WebLockMonitor | null;
  workerLockMonitor: WebLockMonitor | null;
};

type ClearedLeaderState = Pick<
  LeaderState,
  "tabId" | "leadershipId" | "tabLockName" | "workerLockName"
>;

type ResetState = {
  requestId: string;
  requestIds: Set<string>;
  participants: Set<string>;
  preparedTabs: Set<string>;
  errors: string[];
  previousLeader: ClearedLeaderState | null;
  promotedLeadershipId: number | null;
  phase: "preparing" | "promoting" | "reconnecting";
};

type StorageResetOutcome = {
  requestId: string;
  success: boolean;
  errorMessage?: string;
  finishedAt: number;
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const brokerInstanceId = createRandomId("broker");
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
const pendingFollowerAttachmentTimers = new Map<string, ReturnType<typeof setTimeout>>();
const followerAttachmentRetryCounts = new Map<string, number>();
const attachedFollowerPorts = new Set<string>();
let warnedStaleInstanceDrop = false;
let replacementElectionInFlight = false;
let replacementElectionGeneration = 0;
let brokerPingTimer: ReturnType<typeof setTimeout> | null = null;
let leaderFailureRetryTimer: ReturnType<typeof setTimeout> | null = null;
let resetState: ResetState | null = null;
const completedStorageResetOutcomes = new Map<string, StorageResetOutcome>();
const failedLeaderRetryAfterByTabId = new Map<string, number>();

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
  const previousTab = tabs.get(message.tabId);
  if (previousTab && previousTab.port !== port) {
    previousTab.port.close();
  }
  clearFollowerAttachmentState(message.tabId);

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

  startBrokerPingTimer();
  post(port, { type: "broker-hello", brokerInstanceId });
  // Redeliver on every hello, including mid-reset rejoins: a requester that
  // reconnects after its reset finished must not wait out the client timeout.
  redeliverFinishedStorageResets(port);
  if (resetState) {
    addTabToActiveReset(message.tabId);
    return message.tabId;
  }
  if (leader && leader.tabId === message.tabId) {
    clearLeader(leader.leadershipId, {
      demoteLeader: false,
      removeLeaderTab: false,
    });
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
  if (message.type === "hello") return;
  if (message.brokerInstanceId !== brokerInstanceId) {
    if (!warnedStaleInstanceDrop) {
      warnedStaleInstanceDrop = true;
      console.warn(
        `[jazz-broker] dropping "${message.type}" from tab ${tabId}: stamped for broker ` +
          `instance ${String(message.brokerInstanceId)}, current is ${brokerInstanceId}. ` +
          "This usually means tabs are running different jazz-tools versions against one broker.",
      );
    }
    return;
  }

  switch (message.type) {
    case "visibility":
      updateVisibility(tabId, message.visibility);
      evictStaleTabs();
      return;
    case "leader-ready": {
      if (!leader || leader.tabId !== tabId || leader.leadershipId !== message.leadershipId) {
        const tab = tabs.get(tabId);
        if (tab) {
          post(tab.port, {
            type: "demote",
            brokerInstanceId,
            leadershipId: message.leadershipId,
          });
        }
        return;
      }
      const activeReset = resetState;
      const leaderTab = tabs.get(tabId);
      if (
        activeReset?.promotedLeadershipId === message.leadershipId &&
        message.bridgelessStorageReset &&
        !leaderTab?.schemaFingerprint
      ) {
        // Fresh namespace: the promoted leader declared it has no client to
        // rebuild a worker bridge from. The wipe is already done before the
        // tab reports ready, so step the placeholder leader down before
        // reporting reset completion.
        clearLeader(message.leadershipId, { demoteLeader: true, removeLeaderTab: false });
        finishStorageReset(activeReset, true);
        return;
      }
      leader.ready = true;
      leader.tabLockName = message.tabLockName;
      leader.workerLockName = message.workerLockName;
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
    }
    case "follower-port-attached":
      if (!leader || leader.tabId !== tabId || leader.leadershipId !== message.leadershipId) return;
      markFollowerPortAttached(message.followerTabId, message.leadershipId);
      return;
    case "follower-port-closed":
      if (!leader || leader.tabId !== tabId || leader.leadershipId !== message.leadershipId) return;
      clearFollowerAttachmentKey(message.followerTabId, message.leadershipId);
      assignFollowerPorts(leader);
      return;
    case "schema-ready":
      handleSchemaReady(tabId, message.schemaFingerprint);
      return;
    case "leader-failed":
      if (resetState?.promotedLeadershipId === message.leadershipId) {
        resetState.errors.push(message.reason);
        removeTab(tabId, { closePort: false, notifyLeader: false });
        removeTabFromActiveReset(tabId);
        leader = null;
        resetState.promotedLeadershipId = null;
        void promoteResetLeader(resetState);
        return;
      }
      if (leader?.tabId === tabId && leader.leadershipId === message.leadershipId) {
        markLeaderCandidateFailed(tabId);
        const cleared = clearLeader(message.leadershipId, {
          demoteLeader: true,
          removeLeaderTab: false,
        });
        scheduleReplacementElection(cleared);
      }
      return;
    case "storage-reset-request":
      startStorageReset(tabId, message.requestId);
      return;
    case "storage-reset-ready":
      handleStorageResetReady(tabId, message.requestId, message.success, message.errorMessage);
      return;
    case "shutdown":
      if (leader?.tabId === tabId) {
        const leadershipId = leader.leadershipId;
        const activeReset = resetState;
        removeTab(tabId, { closePort: false, notifyLeader: false });
        const cleared = clearLeader(leadershipId, {
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
        removeTab(tabId, { closePort: false, notifyLeader: true });
        removeTabFromActiveReset(tabId);
      }
      resetIfIdle();
      return;
    case "broker-pong":
      {
        const tab = tabs.get(tabId);
        if (!tab) return;
        tab.lastPongAt = Date.now();
      }
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
  if (replacementElectionInFlight) return;
  if (leader?.ready || tabs.size === 0) return;
  if (leader && !namespace?.schemaFingerprint) return;

  const candidate = selectLeaderCandidate(eligibleLeaderCandidates());
  if (!candidate) {
    scheduleLeaderFailureRetryElection();
    return;
  }

  if (leader) {
    const currentLeaderTab = tabs.get(leader.tabId);
    if (currentLeaderTab?.schemaFingerprint === namespace?.schemaFingerprint) {
      return;
    }
    if (candidate.tabId === leader.tabId) {
      return;
    }
    clearLeader(leader.leadershipId, {
      demoteLeader: true,
      removeLeaderTab: false,
    });
  }

  const tab = tabs.get(candidate.tabId);
  if (!tab) return;
  const lockNames = currentLeaderLockNames();
  if (!lockNames) return;

  currentLeadershipId += 1;
  leader = {
    tabId: tab.tabId,
    leadershipId: currentLeadershipId,
    ready: false,
    tabLockName: lockNames.tabLockName,
    workerLockName: lockNames.workerLockName,
    tabLockMonitor: null,
    workerLockMonitor: null,
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
  clearAllFollowerAttachmentState();
  resetState = null;
  replacementElectionGeneration += 1;
  replacementElectionInFlight = false;
  failedLeaderRetryAfterByTabId.clear();
  stopLeaderFailureRetryTimer();
  stopBrokerPingTimer();
}

function removeTab(
  tabId: string,
  options: { closePort: boolean; notifyLeader: boolean },
): TabState | null {
  const tab = tabs.get(tabId);
  if (!tab) return null;

  if (options.notifyLeader && leader && leader.tabId !== tabId) {
    notifyLeaderToDetachFollower(tabId, leader.leadershipId);
  }

  tabs.delete(tabId);
  failedLeaderRetryAfterByTabId.delete(tabId);
  clearFollowerAttachmentState(tabId);
  reelectSchemaFingerprintIfUnheld(tab);
  if (options.closePort) {
    tab.port.close();
  }
  return tab;
}

function notifyLeaderToDetachFollower(followerTabId: string, leadershipId: number): void {
  const leaderTab = leader ? tabs.get(leader.tabId) : null;
  if (!leaderTab) return;
  post(leaderTab.port, {
    type: "detach-follower-port",
    brokerInstanceId,
    followerTabId,
    leadershipId,
  });
}

function clearFollowerAttachmentState(followerTabId: string): void {
  for (const key of pendingFollowerAttachments) {
    if (key.endsWith(`:${followerTabId}`)) {
      clearPendingFollowerAttachment(key);
    }
  }
  for (const key of attachedFollowerPorts) {
    if (key.endsWith(`:${followerTabId}`)) {
      attachedFollowerPorts.delete(key);
    }
  }
}

function clearFollowerAttachmentKey(followerTabId: string, leadershipId: number): void {
  const key = followerAttachmentKey(followerTabId, leadershipId);
  clearPendingFollowerAttachment(key);
  attachedFollowerPorts.delete(key);
}

function clearAllFollowerAttachmentState(): void {
  for (const timer of pendingFollowerAttachmentTimers.values()) {
    clearTimeout(timer);
  }
  pendingFollowerAttachmentTimers.clear();
  pendingFollowerAttachments.clear();
  followerAttachmentRetryCounts.clear();
  attachedFollowerPorts.clear();
}

function clearPendingFollowerAttachment(key: string): void {
  pendingFollowerAttachments.delete(key);
  followerAttachmentRetryCounts.delete(key);
  const timer = pendingFollowerAttachmentTimers.get(key);
  if (timer) {
    clearTimeout(timer);
    pendingFollowerAttachmentTimers.delete(key);
  }
}

function handleSchemaReady(tabId: string, schemaFingerprint: string): void {
  if (!namespace) return;
  const tab = tabs.get(tabId);
  if (!tab) return;

  if (!namespace.schemaFingerprint) {
    namespace.schemaFingerprint = schemaFingerprint;
  }

  tab.schemaFingerprint = schemaFingerprint;

  if (namespace.schemaFingerprint !== schemaFingerprint) {
    blockTabForSchemaMismatch(tab);
    return;
  }

  if (leader?.ready) {
    assignFollowerPorts(leader);
    return;
  }
  electIfNeeded();
}

// A mismatching tab stays connected (and keeps answering pings) so it can be
// adopted once the canonical fingerprint is re-elected; it is excluded from
// leadership and follower ports purely by its non-canonical fingerprint.
function blockTabForSchemaMismatch(tab: TabState): void {
  post(tab.port, {
    type: "schema-blocked",
    brokerInstanceId,
    reason: "incompatible persistent browser schema",
  });

  if (leader?.tabId === tab.tabId) {
    const cleared = clearLeader(leader.leadershipId, {
      demoteLeader: true,
      removeLeaderTab: false,
    });
    scheduleReplacementElection(cleared);
  }
}

// The canonical fingerprint is held by the tabs that reported it. When the
// last holder departs, the namespace re-elects the earliest-connected
// remaining fingerprint so schema-blocked tabs can recover without reloading.
function reelectSchemaFingerprintIfUnheld(departed: TabState): void {
  if (!namespace?.schemaFingerprint) return;
  if (departed.schemaFingerprint !== namespace.schemaFingerprint) return;
  for (const tab of tabs.values()) {
    if (tab.schemaFingerprint === namespace.schemaFingerprint) return;
  }

  let next: string | null = null;
  for (const tab of tabs.values()) {
    if (tab.schemaFingerprint) {
      next = tab.schemaFingerprint;
      break;
    }
  }
  namespace.schemaFingerprint = next;
  if (!next) return;

  if (leader?.ready) {
    assignFollowerPorts(leader);
  } else {
    electIfNeeded();
  }
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
      handleLeaderLockReleased(nextLeader.leadershipId);
    },
    onError: () => {
      handleLeaderLockReleased(nextLeader.leadershipId);
    },
  });
  nextLeader.workerLockMonitor = monitorWebLockRelease(nextLeader.workerLockName, {
    onGranted: () => {
      handleLeaderLockReleased(nextLeader.leadershipId);
    },
    onError: () => {
      handleLeaderLockReleased(nextLeader.leadershipId);
    },
  });
}

function handleLeaderLockReleased(leadershipId: number): void {
  if (!leader || leader.leadershipId !== leadershipId) return;
  const cleared = clearLeader(leadershipId, { demoteLeader: true, removeLeaderTab: true });
  scheduleReplacementElection(cleared);
}

function clearLeader(
  leadershipId: number,
  options: { demoteLeader: boolean; removeLeaderTab: boolean },
): ClearedLeaderState | null {
  const current = leader;
  if (!current || current.leadershipId !== leadershipId) return null;
  cancelLeaderMonitors(current);
  clearAllFollowerAttachmentState();

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
    removeTab(current.tabId, { closePort: false, notifyLeader: false });
  }
  leader = null;
  return {
    tabId: current.tabId,
    leadershipId: current.leadershipId,
    tabLockName: current.tabLockName,
    workerLockName: current.workerLockName,
  };
}

function cancelLeaderMonitors(current: LeaderState): void {
  current.tabLockMonitor?.cancel();
  current.tabLockMonitor = null;
  current.workerLockMonitor?.cancel();
  current.workerLockMonitor = null;
}

function startStorageReset(requestingTabId: string, requestId: string): void {
  if (resetState) {
    resetState.requestIds.add(requestId);
    const tab = tabs.get(requestingTabId);
    if (tab) {
      post(tab.port, {
        type: "storage-reset-started",
        brokerInstanceId,
        requestId,
      });
    }
    return;
  }

  const previousLeader = leader
    ? clearLeader(leader.leadershipId, {
        demoteLeader: false,
        removeLeaderTab: false,
      })
    : null;

  const leadershipId = previousLeader?.leadershipId ?? currentLeadershipId;
  resetState = {
    requestId,
    requestIds: new Set([requestId]),
    participants: new Set(tabs.keys()),
    preparedTabs: new Set(),
    errors: [],
    previousLeader,
    promotedLeadershipId: null,
    phase: "preparing",
  };

  const requestingTab = tabs.get(requestingTabId);
  if (requestingTab) {
    post(requestingTab.port, {
      type: "storage-reset-started",
      brokerInstanceId,
      requestId,
    });
  }

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

  if (!activeReset.participants.has(tabId)) {
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
  for (const participant of activeReset.participants) {
    if (!activeReset.preparedTabs.has(participant)) return;
  }

  activeReset.phase = "promoting";
  void (async () => {
    await waitForPreviousLeaderLocks(activeReset.previousLeader, () => resetState === activeReset);
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
  const lockNames = currentLeaderLockNames();
  if (!lockNames) {
    finishStorageReset(activeReset, false, "No connected tab is available to reset storage");
    return;
  }

  currentLeadershipId += 1;
  activeReset.promotedLeadershipId = currentLeadershipId;
  leader = {
    tabId: tab.tabId,
    leadershipId: currentLeadershipId,
    ready: false,
    tabLockName: lockNames.tabLockName,
    workerLockName: lockNames.workerLockName,
    tabLockMonitor: null,
    workerLockMonitor: null,
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

  const outcomes = rememberStorageResetOutcomes(completedReset.requestIds, success, errorMessage);
  for (const tab of tabs.values()) {
    for (const outcome of outcomes) {
      postStorageResetOutcome(tab.port, outcome);
    }
  }

  if (!success) {
    electIfNeeded();
  }
}

function rememberStorageResetOutcomes(
  requestIds: Iterable<string>,
  success: boolean,
  errorMessage?: string,
): StorageResetOutcome[] {
  const now = Date.now();
  const outcomes: StorageResetOutcome[] = [];
  for (const requestId of requestIds) {
    const outcome: StorageResetOutcome = {
      requestId,
      success,
      ...(errorMessage ? { errorMessage } : {}),
      finishedAt: now,
    };
    // Delete first: re-setting an existing key keeps its Map position, which
    // would make the size eviction below treat a re-finished id as oldest.
    completedStorageResetOutcomes.delete(requestId);
    completedStorageResetOutcomes.set(requestId, outcome);
    outcomes.push(outcome);
  }
  pruneCompletedStorageResetOutcomes(now);
  return outcomes;
}

function redeliverFinishedStorageResets(port: MessagePort): void {
  pruneCompletedStorageResetOutcomes();
  for (const outcome of completedStorageResetOutcomes.values()) {
    postStorageResetOutcome(port, outcome);
  }
}

function postStorageResetOutcome(port: MessagePort, outcome: StorageResetOutcome): void {
  post(port, {
    type: "storage-reset-finished",
    brokerInstanceId,
    requestId: outcome.requestId,
    success: outcome.success,
    ...(outcome.errorMessage ? { errorMessage: outcome.errorMessage } : {}),
  });
}

function pruneCompletedStorageResetOutcomes(now = Date.now()): void {
  for (const [requestId, outcome] of completedStorageResetOutcomes) {
    if (now - outcome.finishedAt > COMPLETED_STORAGE_RESET_OUTCOME_TTL_MS) {
      completedStorageResetOutcomes.delete(requestId);
    }
  }

  while (completedStorageResetOutcomes.size > MAX_COMPLETED_STORAGE_RESET_OUTCOMES) {
    const oldestRequestId = completedStorageResetOutcomes.keys().next().value;
    if (!oldestRequestId) return;
    completedStorageResetOutcomes.delete(oldestRequestId);
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
  const electionGeneration = ++replacementElectionGeneration;

  void (async () => {
    try {
      await waitForPreviousLeaderLocks(
        previousLeader,
        () =>
          replacementElectionInFlight &&
          replacementElectionGeneration === electionGeneration &&
          leader === null,
      );
    } finally {
      if (replacementElectionGeneration === electionGeneration) {
        replacementElectionInFlight = false;
      }
    }
    if (replacementElectionGeneration !== electionGeneration) return;
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
  removeTab(tabId, { closePort: true, notifyLeader: true });

  if (leader?.tabId === tabId) {
    const leadershipId = leader.leadershipId;
    const activeReset = resetState;
    const cleared = clearLeader(leadershipId, {
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
  shouldForceTakeover: () => boolean = () => true,
): Promise<void> {
  if (!previousLeader?.tabLockName || !previousLeader.workerLockName) {
    return;
  }

  if (!shouldForceTakeover()) return;

  const takeoverLockNames = [previousLeader.tabLockName, previousLeader.workerLockName].filter(
    (lockName): lockName is string => lockName !== null,
  );

  if (await acquireAndReleaseLocks(takeoverLockNames)) {
    return;
  }

  await sleep(namespace?.forceTakeoverTimeoutMs ?? DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS);
  if (!shouldForceTakeover()) return;
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
    markFollowerPortPending(follower.tabId, nextLeader.leadershipId);
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

function markFollowerPortPending(followerTabId: string, leadershipId: number): void {
  const key = followerAttachmentKey(followerTabId, leadershipId);
  pendingFollowerAttachments.add(key);
  const retryCount = followerAttachmentRetryCounts.get(key) ?? 0;
  const timeoutMs = Math.min(
    INITIAL_FOLLOWER_ATTACHMENT_TIMEOUT_MS * 2 ** retryCount,
    MAX_FOLLOWER_ATTACHMENT_TIMEOUT_MS,
  );
  const timer = setTimeout(() => {
    pendingFollowerAttachmentTimers.delete(key);
    if (!pendingFollowerAttachments.delete(key)) return;
    if (!leader || !leader.ready || leader.leadershipId !== leadershipId) return;
    if (!tabs.has(followerTabId)) return;
    followerAttachmentRetryCounts.set(key, retryCount + 1);
    assignFollowerPorts(leader);
  }, timeoutMs);
  pendingFollowerAttachmentTimers.set(key, timer);
}

function eligibleLeaderCandidates(): BrowserBrokerCandidate[] {
  const schemaFingerprint = namespace?.schemaFingerprint;
  return [...tabs.values()].filter((tab) => {
    if (isLeaderCandidateInFailureBackoff(tab.tabId)) {
      return false;
    }
    return !schemaFingerprint || tab.schemaFingerprint === schemaFingerprint;
  });
}

function markLeaderCandidateFailed(tabId: string): void {
  failedLeaderRetryAfterByTabId.set(tabId, Date.now() + LEADER_FAILURE_RETRY_BACKOFF_MS);
}

function isLeaderCandidateInFailureBackoff(tabId: string, now = Date.now()): boolean {
  const retryAfter = failedLeaderRetryAfterByTabId.get(tabId);
  if (retryAfter === undefined) return false;
  if (retryAfter <= now) {
    failedLeaderRetryAfterByTabId.delete(tabId);
    return false;
  }
  return true;
}

function scheduleLeaderFailureRetryElection(now = Date.now()): void {
  if (leaderFailureRetryTimer || resetState || replacementElectionInFlight || leader?.ready) return;

  let retryAt: number | null = null;
  for (const [tabId, candidateRetryAt] of failedLeaderRetryAfterByTabId) {
    if (!tabs.has(tabId)) {
      failedLeaderRetryAfterByTabId.delete(tabId);
      continue;
    }
    retryAt = retryAt === null ? candidateRetryAt : Math.min(retryAt, candidateRetryAt);
  }
  if (retryAt === null) return;

  leaderFailureRetryTimer = setTimeout(
    () => {
      leaderFailureRetryTimer = null;
      electIfNeeded();
    },
    Math.max(0, retryAt - now),
  );
}

function stopLeaderFailureRetryTimer(): void {
  if (!leaderFailureRetryTimer) return;
  clearTimeout(leaderFailureRetryTimer);
  leaderFailureRetryTimer = null;
}

function currentLeaderLockNames(): { tabLockName: string; workerLockName: string } | null {
  if (!namespace) return null;
  return {
    tabLockName: `jazz-leader-tab:${namespace.appId}:${namespace.dbName}`,
    workerLockName: `jazz-leader-worker:${namespace.appId}:${namespace.dbName}`,
  };
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
  clearPendingFollowerAttachment(key);

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

function normalizeForceTakeoverTimeout(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return DEFAULT_FORCE_TAKEOVER_TIMEOUT_MS;
  }
  return Math.max(0, Math.floor(value));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
