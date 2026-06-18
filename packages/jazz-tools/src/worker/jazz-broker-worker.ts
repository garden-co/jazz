import type {
  BrowserBrokerControlMessage,
  BrowserBrokerTabMessage,
} from "../runtime/browser-broker-protocol.js";
import { createRandomId, stringifyError } from "../runtime/browser-broker-protocol.js";
import {
  readWorkerRuntimeWasmUrl,
  resolveRuntimeConfigWasmUrl,
} from "../runtime/runtime-config.js";
import {
  monitorWebLockRelease,
  stealAndReleaseWebLock,
  tryAcquireWebLock,
  type WebLockMonitor,
} from "../runtime/leader-lock.js";

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
  location?: { href?: string };
};

type BrokerElectionModule = {
  default?: (input?: unknown) => Promise<unknown>;
  initSync?: (input?: unknown) => unknown;
  BrokerElection: new (brokerInstanceId: string) => BrokerElectionBinding;
};

type BrokerElectionBinding = {
  handleEvent(event: BrokerEvent): BrokerEffect[];
  snapshot(): unknown;
};

type VitestBrowserRunner = {
  wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T>;
};

type BrokerVisibility = "visible" | "hidden";

// Mirrors the serde-wasm-bindgen event/effect shapes in
// crates/jazz-wasm/src/broker_election.rs. Keep the Rust serialization tests
// and these unions in sync until the protocol declarations are generated.
type BrokerTimerId =
  | { type: "brokerPing" }
  | { type: "leaderFailureRetry" }
  | { type: "followerAttachment"; followerTabId: string; leadershipId: number }
  | { type: "previousLeaderLocksForceTakeover"; electionId: number };

type BrokerEvent =
  | {
      type: "tabConnected";
      tabId: string;
      appId: string;
      dbName: string;
      fingerprint: string;
      visibility: BrokerVisibility;
      nowMs: number;
      forceTakeoverTimeoutMs?: number;
      brokerPingIntervalMs?: number;
      brokerPongTimeoutMs?: number;
    }
  | { type: "visibilityChanged"; tabId: string; visibility: BrokerVisibility; nowMs: number }
  | { type: "schemaReported"; tabId: string; schemaFingerprint: string }
  | {
      type: "leaderReady";
      tabId: string;
      leadershipId: number;
      tabLockName: string;
      workerLockName: string;
      bridgelessStorageReset: boolean;
      nowMs: number;
    }
  | { type: "leaderFailed"; tabId: string; leadershipId: number; reason: string; nowMs: number }
  | {
      type: "followerPortAttached";
      leaderTabId: string;
      followerTabId: string;
      leadershipId: number;
      nowMs: number;
    }
  | {
      type: "followerPortClosed";
      leaderTabId: string;
      followerTabId: string;
      leadershipId: number;
      nowMs: number;
    }
  | { type: "storageResetRequested"; tabId: string; requestId: string; nowMs: number }
  | {
      type: "storageResetReady";
      tabId: string;
      requestId: string;
      success: boolean;
      errorMessage?: string;
      nowMs: number;
    }
  | { type: "shutdown"; tabId: string; nowMs: number }
  | { type: "brokerPong"; tabId: string; nowMs: number }
  | { type: "timerFired"; timerId: BrokerTimerId; nowMs: number }
  | { type: "leaderLockReleased"; leadershipId: number; nowMs: number }
  | { type: "previousLeaderLocksReleased"; electionId: number; nowMs: number }
  | { type: "forceTakeoverComplete"; electionId: number; nowMs: number }
  | { type: "forceTakeoverFailed"; electionId: number; reason: string; nowMs: number };

type BrokerEffect =
  | { type: "sendToTab"; tabId: string; message: BrowserBrokerControlMessage }
  | { type: "closeTabPort"; tabId: string }
  | { type: "closeReplacedTabPort"; tabId: string }
  | { type: "broadcast"; message: BrowserBrokerControlMessage }
  | { type: "armTimer"; timerId: BrokerTimerId; delayMs: number }
  | { type: "cancelTimer"; timerId: BrokerTimerId }
  | {
      type: "startLeaderLockMonitor";
      leadershipId: number;
      tabLockName: string;
      workerLockName: string;
    }
  | { type: "cancelLeaderLockMonitor"; leadershipId: number }
  | {
      type: "waitForPreviousLeaderLocks";
      electionId: number;
      tabLockName: string;
      workerLockName: string;
    }
  | {
      type: "stealPreviousLeaderLocks";
      electionId: number;
      tabLockName: string;
      workerLockName: string;
    }
  | {
      type: "assignFollowerPort";
      leaderTabId: string;
      followerTabId: string;
      leadershipId: number;
    };

const workerGlobal = globalThis as SharedWorkerGlobal;
const brokerInstanceId = createRandomId("broker");
const ports = new Map<string, MessagePort>();
const pendingConnects: Array<{
  port: MessagePort;
  message: Extract<BrowserBrokerTabMessage, { type: "hello" }>;
}> = [];
const timers = new Map<string, ReturnType<typeof setTimeout>>();
const leaderLockMonitors = new Map<number, WebLockMonitor[]>();
const replacedPorts = new Map<string, MessagePort>();
let election: BrokerElectionBinding | null = null;
let startupError: Error | null = null;
let startup: Promise<void> | null = null;
let warnedStaleInstanceDrop = false;

// Vite's browser test transform may wrap dynamic imports in worker bundles.
// Production workers never read this property, but defining the no-op keeps the
// same worker entry usable in Vitest without a test-only bundle.
ensureVitestWorkerImportShim();
void ensureStartup();

workerGlobal.onconnect = (event) => {
  const port = event.ports[0];
  if (!port) return;

  let tabId: string | null = null;

  port.addEventListener("message", (messageEvent) => {
    const message = messageEvent.data as BrowserBrokerTabMessage;
    if (!message || typeof message !== "object") return;

    if (message.type === "hello") {
      tabId = message.tabId;
      void handleHello(port, message);
      return;
    }

    if (!tabId) return;
    handleTabMessage(tabId, message);
  });
  port.start();
};

async function bootstrapElection(): Promise<void> {
  const startedAt = performanceNow();
  try {
    const wasmModule = (await import("jazz-wasm")) as unknown as BrokerElectionModule;
    await ensureWasmInitialized(wasmModule);
    election = new wasmModule.BrokerElection(brokerInstanceId);
    markBrokerTiming("wasm-init", startedAt);

    for (const pending of pendingConnects.splice(0)) {
      runEvent(tabConnectedEvent(pending.message), {
        currentPort: pending.port,
        tabId: pending.message.tabId,
      });
    }
  } catch (error) {
    startupError = error instanceof Error ? error : new Error(String(error));
    for (const pending of pendingConnects.splice(0)) {
      post(pending.port, {
        type: "unsupported",
        brokerInstanceId,
        reason: `browser broker startup failed: ${startupError.message}`,
      });
      pending.port.close();
    }
  }
}

function ensureStartup(): Promise<void> {
  if (!startup || startupError) {
    startupError = null;
    startup = bootstrapElection();
  }
  return startup;
}

async function handleHello(
  port: MessagePort,
  message: Extract<BrowserBrokerTabMessage, { type: "hello" }>,
): Promise<void> {
  if (!election) {
    pendingConnects.push({ port, message });
    await ensureStartup();
    return;
  }

  runEvent(tabConnectedEvent(message), { currentPort: port, tabId: message.tabId });
}

function handleTabMessage(tabId: string, message: BrowserBrokerTabMessage): void {
  if (message.type === "hello") return;
  if (message.brokerInstanceId !== brokerInstanceId) {
    if (!warnedStaleInstanceDrop) {
      warnedStaleInstanceDrop = true;
      console.warn(
        `[jazz-broker] dropping "${message.type}" from tab ${tabId}: stamped for broker ` +
          `instance ${String(message.brokerInstanceId)}, current is ${brokerInstanceId}.`,
      );
    }
    return;
  }

  switch (message.type) {
    case "visibility":
      runEvent({
        type: "visibilityChanged",
        tabId,
        visibility: message.visibility,
        nowMs: Date.now(),
      });
      return;
    case "leader-ready":
      runEvent({
        type: "leaderReady",
        tabId,
        leadershipId: message.leadershipId,
        tabLockName: message.tabLockName,
        workerLockName: message.workerLockName,
        bridgelessStorageReset: message.bridgelessStorageReset === true,
        nowMs: Date.now(),
      });
      return;
    case "follower-port-attached":
      runEvent({
        type: "followerPortAttached",
        leaderTabId: tabId,
        followerTabId: message.followerTabId,
        leadershipId: message.leadershipId,
        nowMs: Date.now(),
      });
      return;
    case "follower-port-closed":
      runEvent({
        type: "followerPortClosed",
        leaderTabId: tabId,
        followerTabId: message.followerTabId,
        leadershipId: message.leadershipId,
        nowMs: Date.now(),
      });
      return;
    case "schema-ready":
      runEvent({ type: "schemaReported", tabId, schemaFingerprint: message.schemaFingerprint });
      return;
    case "leader-failed":
      runEvent({
        type: "leaderFailed",
        tabId,
        leadershipId: message.leadershipId,
        reason: message.reason,
        nowMs: Date.now(),
      });
      return;
    case "storage-reset-request":
      runEvent({
        type: "storageResetRequested",
        tabId,
        requestId: message.requestId,
        nowMs: Date.now(),
      });
      return;
    case "storage-reset-ready":
      runEvent({
        type: "storageResetReady",
        tabId,
        requestId: message.requestId,
        success: message.success,
        ...(message.errorMessage ? { errorMessage: message.errorMessage } : {}),
        nowMs: Date.now(),
      });
      return;
    case "shutdown":
      runEvent({ type: "shutdown", tabId, nowMs: Date.now() });
      ports.delete(tabId);
      return;
    case "broker-pong":
      runEvent({ type: "brokerPong", tabId, nowMs: Date.now() });
      return;
  }
}

function tabConnectedEvent(
  message: Extract<BrowserBrokerTabMessage, { type: "hello" }>,
): BrokerEvent {
  return {
    type: "tabConnected",
    tabId: message.tabId,
    appId: message.appId,
    dbName: message.dbName,
    fingerprint: message.fingerprint,
    visibility: message.visibility,
    nowMs: Date.now(),
    forceTakeoverTimeoutMs: sanitizeOptionalTimeoutMs(message.forceTakeoverTimeoutMs),
    brokerPingIntervalMs: sanitizeOptionalTimeoutMs(message.brokerPingIntervalMs),
    brokerPongTimeoutMs: sanitizeOptionalTimeoutMs(message.brokerPongTimeoutMs),
  };
}

function runEvent(
  event: BrokerEvent,
  options: { currentPort?: MessagePort; tabId?: string } = {},
): void {
  if (!election) return;

  if (event.type === "tabConnected" && options.currentPort && options.tabId) {
    const previous = ports.get(options.tabId);
    if (previous && previous !== options.currentPort) {
      replacedPorts.set(options.tabId, previous);
    }
    ports.set(options.tabId, options.currentPort);
  }

  let effects: BrokerEffect[];
  try {
    effects = election.handleEvent(event);
  } catch (error) {
    console.error("[jazz-broker] Rust broker event failed", error);
    if (event.type === "tabConnected" && options.currentPort) {
      post(options.currentPort, {
        type: "unsupported",
        brokerInstanceId,
        reason: `browser broker event failed: ${stringifyError(error)}`,
      });
      options.currentPort.close();
    }
    return;
  }

  executeEffects(effects);
}

function executeEffects(effects: BrokerEffect[]): void {
  for (const effect of effects) {
    switch (effect.type) {
      case "sendToTab": {
        const port = ports.get(effect.tabId);
        if (port) post(port, effect.message);
        break;
      }
      case "closeTabPort":
        closeTabPort(effect.tabId);
        break;
      case "closeReplacedTabPort":
        closeReplacedTabPort(effect.tabId);
        break;
      case "broadcast":
        for (const port of ports.values()) {
          post(port, effect.message);
        }
        break;
      case "armTimer":
        armTimer(effect.timerId, effect.delayMs);
        break;
      case "cancelTimer":
        cancelTimer(effect.timerId);
        break;
      case "startLeaderLockMonitor":
        startLeaderLockMonitor(effect);
        break;
      case "cancelLeaderLockMonitor":
        cancelLeaderLockMonitor(effect.leadershipId);
        break;
      case "waitForPreviousLeaderLocks":
        void waitForPreviousLeaderLocks(effect);
        break;
      case "stealPreviousLeaderLocks":
        void stealPreviousLeaderLocks(effect);
        break;
      case "assignFollowerPort":
        assignFollowerPort(effect);
        break;
    }
  }
}

function closeTabPort(tabId: string): void {
  const port = ports.get(tabId);
  const replaced = replacedPorts.get(tabId);
  if (replaced && port) {
    replacedPorts.delete(tabId);
    ports.set(tabId, replaced);
    port.close();
    return;
  }

  ports.delete(tabId);
  port?.close();
}

function closeReplacedTabPort(tabId: string): void {
  const replaced = replacedPorts.get(tabId);
  if (!replaced) return;
  replacedPorts.delete(tabId);
  replaced.close();
}

function armTimer(timerId: BrokerTimerId, delayMs: number): void {
  const key = timerKey(timerId);
  cancelTimer(timerId);
  timers.set(
    key,
    setTimeout(() => {
      timers.delete(key);
      runEvent({ type: "timerFired", timerId, nowMs: Date.now() });
    }, delayMs),
  );
}

function cancelTimer(timerId: BrokerTimerId): void {
  const key = timerKey(timerId);
  const timer = timers.get(key);
  if (!timer) return;
  clearTimeout(timer);
  timers.delete(key);
}

function startLeaderLockMonitor(effect: Extract<BrokerEffect, { type: "startLeaderLockMonitor" }>) {
  cancelLeaderLockMonitor(effect.leadershipId);
  const onReleased = () => {
    runEvent({
      type: "leaderLockReleased",
      leadershipId: effect.leadershipId,
      nowMs: Date.now(),
    });
  };
  leaderLockMonitors.set(effect.leadershipId, [
    monitorWebLockRelease(effect.tabLockName, { onGranted: onReleased, onError: onReleased }),
    monitorWebLockRelease(effect.workerLockName, { onGranted: onReleased, onError: onReleased }),
  ]);
}

function cancelLeaderLockMonitor(leadershipId: number): void {
  const monitors = leaderLockMonitors.get(leadershipId);
  if (!monitors) return;
  leaderLockMonitors.delete(leadershipId);
  for (const monitor of monitors) {
    monitor.cancel();
  }
}

async function waitForPreviousLeaderLocks(
  effect: Extract<BrokerEffect, { type: "waitForPreviousLeaderLocks" }>,
): Promise<void> {
  if (await acquireAndReleaseLocks([effect.tabLockName, effect.workerLockName])) {
    runEvent({
      type: "previousLeaderLocksReleased",
      electionId: effect.electionId,
      nowMs: Date.now(),
    });
  }
  // If the probe could not acquire both locks, Rust already has a force-takeover
  // timer armed for this election id. The timer drives the next explicit event.
}

async function stealPreviousLeaderLocks(
  effect: Extract<BrokerEffect, { type: "stealPreviousLeaderLocks" }>,
): Promise<void> {
  const results = await Promise.allSettled([
    stealAndReleaseWebLock(effect.tabLockName),
    stealAndReleaseWebLock(effect.workerLockName),
  ]);
  const failed = results.find((result) => result.status === "rejected");
  if (failed) {
    runEvent({
      type: "forceTakeoverFailed",
      electionId: effect.electionId,
      reason: stringifyError(failed.reason),
      nowMs: Date.now(),
    });
    return;
  }
  runEvent({
    type: "forceTakeoverComplete",
    electionId: effect.electionId,
    nowMs: Date.now(),
  });
}

async function acquireAndReleaseLocks(lockNames: readonly string[]): Promise<boolean> {
  const leases = await Promise.all(lockNames.map((lockName) => tryAcquireWebLock(lockName)));
  for (const lease of leases) {
    lease?.release();
  }
  return leases.every((lease) => lease !== null);
}

function sanitizeOptionalTimeoutMs(value: number | undefined): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return undefined;
  }
  const normalized = Math.max(0, Math.floor(value));
  return normalized > 0 ? normalized : undefined;
}

function assignFollowerPort(effect: Extract<BrokerEffect, { type: "assignFollowerPort" }>): void {
  const leaderPort = ports.get(effect.leaderTabId);
  const followerPort = ports.get(effect.followerTabId);
  if (!leaderPort || !followerPort) return;

  const channel = new MessageChannel();
  post(
    leaderPort,
    {
      type: "attach-follower-port",
      brokerInstanceId,
      followerTabId: effect.followerTabId,
      leadershipId: effect.leadershipId,
      port: channel.port1,
    },
    [channel.port1],
  );
  post(
    followerPort,
    {
      type: "use-follower-port",
      brokerInstanceId,
      leaderTabId: effect.leaderTabId,
      leadershipId: effect.leadershipId,
      port: channel.port2,
    },
    [channel.port2],
  );
}

async function ensureWasmInitialized(wasmModule: BrokerElectionModule): Promise<void> {
  if (typeof wasmModule.default !== "function") return;

  const locationHref = workerGlobal.location?.href;
  const wasmUrl =
    resolveRuntimeConfigWasmUrl(import.meta.url, locationHref, undefined) ??
    readWorkerRuntimeWasmUrl(locationHref);

  if (wasmUrl) {
    await wasmModule.default({ module_or_path: wasmUrl });
    return;
  }

  await runWithRootRelativeFetchSupport(() => wasmModule.default?.());
}

async function runWithRootRelativeFetchSupport<T>(operation: () => Promise<T> | T): Promise<T> {
  const globalRef = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const originalFetch = globalRef.fetch;
  const origin = workerGlobal.location?.href ? new URL(workerGlobal.location.href).origin : null;
  if (typeof originalFetch !== "function" || !origin) return await operation();

  const patchedFetch: typeof fetch = (input, init) =>
    originalFetch(
      typeof input === "string" && input.startsWith("/")
        ? new URL(input, origin).toString()
        : input,
      init,
    );
  globalRef.fetch = patchedFetch;
  try {
    return await operation();
  } finally {
    globalRef.fetch = originalFetch;
  }
}

function timerKey(timerId: BrokerTimerId): string {
  return JSON.stringify(timerId);
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

function performanceNow(): number {
  return typeof performance !== "undefined" && typeof performance.now === "function"
    ? performance.now()
    : Date.now();
}

function ensureVitestWorkerImportShim(): void {
  if (!isVitestEnvironment()) return;

  const globalRef = globalThis as typeof globalThis & {
    __vitest_browser_runner__?: VitestBrowserRunner;
  };
  if (globalRef.__vitest_browser_runner__) return;
  globalRef.__vitest_browser_runner__ = {
    wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T> {
      return loader();
    },
  };
}

function isVitestEnvironment(): boolean {
  const processLike =
    typeof process !== "undefined" ? (process as { env?: { VITEST?: string } }) : undefined;
  if (processLike?.env?.VITEST) return true;

  const importMeta = import.meta as ImportMeta & {
    env?: { MODE?: string; VITEST?: boolean | string };
  };
  return importMeta.env?.MODE === "test" || Boolean(importMeta.env?.VITEST);
}

function markBrokerTiming(label: string, startedAt: number): void {
  const durationMs = performanceNow() - startedAt;
  (globalThis as typeof globalThis & { performance?: Performance }).performance?.measure?.(
    `jazz-broker:${label}`,
    {
      start: startedAt,
      duration: durationMs,
    },
  );
}
