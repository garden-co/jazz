import {
  selectLeaderCandidate,
  type BrowserBrokerCandidate,
  type BrowserBrokerControlMessage,
  type BrowserBrokerTabMessage,
  type BrowserBrokerVisibility,
} from "../runtime/browser-broker-protocol.js";

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
};

type TabState = BrowserBrokerCandidate & {
  appId: string;
  dbName: string;
  fingerprint: string;
  port: MessagePort;
};

type LeaderState = {
  tabId: string;
  term: number;
  ready: boolean;
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const brokerEpoch = createBrokerId("epoch");
const tabs = new Map<string, TabState>();
let namespace: { appId: string; dbName: string; fingerprint: string } | null = null;
let leader: LeaderState | null = null;
let currentTerm = 0;

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
  });

  post(port, { type: "broker-hello", brokerEpoch });
  if (leader?.ready) {
    post(port, {
      type: "leader-ready",
      brokerEpoch,
      leaderTabId: leader.tabId,
      term: leader.term,
    });
  } else {
    electIfNeeded();
  }

  return message.tabId;
}

function handleTabMessage(tabId: string, message: BrowserBrokerTabMessage): void {
  switch (message.type) {
    case "visibility":
      updateVisibility(tabId, message.visibility);
      return;
    case "leader-ready":
      if (!leader || leader.tabId !== tabId || leader.term !== message.term) return;
      leader.ready = true;
      announceLeaderReady(leader);
      return;
    case "leader-failed":
      if (leader?.tabId === tabId && leader.term === message.term) {
        leader = null;
        electIfNeeded();
      }
      return;
    case "shutdown":
      tabs.delete(tabId);
      if (leader?.tabId === tabId) {
        leader = null;
        electIfNeeded();
      }
      return;
    case "broker-pong":
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
  };

  post(tab.port, {
    type: "become-leader",
    brokerEpoch,
    term: currentTerm,
  });
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

function post(port: MessagePort, message: BrowserBrokerControlMessage): void {
  port.postMessage(message);
}

function createBrokerId(prefix: string): string {
  const cryptoObj = globalThis.crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return `${prefix}-${cryptoObj.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}
