import type { RuntimeSourcesConfig, Session } from "./context.js";

export const BROKER_CONTROL_PROTOCOL_VERSION = "jazz-browser-broker-v1";
export const BROWSER_STORAGE_FORMAT_VERSION = "opfs-btree-v1";

export type BrowserBrokerVisibility = "visible" | "hidden";
export type BrowserBrokerRole = "leader" | "follower";

export interface BrowserBrokerCandidate {
  tabId: string;
  visibility: BrowserBrokerVisibility;
  lastVisibleAt: number;
}

export interface BrowserBrokerFingerprintInput {
  appId: string;
  dbName: string;
  env: string;
  userBranch: string;
  serverUrl?: string | null;
  schemaHash?: string | null;
  authClass: string;
  runtimeSourceIdentity: string;
  persistentDriverNamespace: string;
  storageFormatVersion?: string;
}

export interface BrowserBrokerHelloMessage {
  type: "hello";
  tabId: string;
  appId: string;
  dbName: string;
  fingerprint: string;
  visibility: BrowserBrokerVisibility;
  forceTakeoverTimeoutMs?: number;
  brokerPingIntervalMs?: number;
  brokerPongTimeoutMs?: number;
}

export interface BrowserBrokerVisibilityMessage {
  type: "visibility";
  visibility: BrowserBrokerVisibility;
}

export interface BrowserBrokerLeaderReadyMessage {
  type: "leader-ready";
  term: number;
  tabLockName: string;
  workerLockName: string;
  compatibilityLockName?: string;
}

export interface BrowserBrokerLeaderFailedMessage {
  type: "leader-failed";
  term: number;
  reason: string;
}

export interface BrowserBrokerFollowerPortAttachedMessage {
  type: "follower-port-attached";
  term: number;
  followerTabId: string;
}

export interface BrowserBrokerStorageResetRequestMessage {
  type: "storage-reset-request";
  requestId: string;
}

export interface BrowserBrokerStorageResetReadyMessage {
  type: "storage-reset-ready";
  requestId: string;
  success: boolean;
  errorMessage?: string;
}

export interface BrowserBrokerShutdownMessage {
  type: "shutdown";
}

export interface BrowserBrokerPongMessage {
  type: "broker-pong";
  brokerEpoch: string;
}

export type BrowserBrokerTabMessage =
  | BrowserBrokerHelloMessage
  | BrowserBrokerVisibilityMessage
  | BrowserBrokerLeaderReadyMessage
  | BrowserBrokerLeaderFailedMessage
  | BrowserBrokerFollowerPortAttachedMessage
  | BrowserBrokerStorageResetRequestMessage
  | BrowserBrokerStorageResetReadyMessage
  | BrowserBrokerShutdownMessage
  | BrowserBrokerPongMessage;

export interface BrokerEpochMessage {
  brokerEpoch: string;
}

export interface BrowserBrokerHelloResponse extends BrokerEpochMessage {
  type: "broker-hello";
}

export interface BrowserBrokerPingMessage extends BrokerEpochMessage {
  type: "broker-ping";
}

export interface BrowserBrokerBecomeLeaderMessage extends BrokerEpochMessage {
  type: "become-leader";
  term: number;
  resetRequestId?: string;
}

export interface BrowserBrokerDemoteMessage extends BrokerEpochMessage {
  type: "demote";
  term: number;
}

export interface BrowserBrokerLeaderReadyAnnouncement extends BrokerEpochMessage {
  type: "leader-ready";
  leaderTabId: string;
  term: number;
}

export interface BrowserBrokerAttachFollowerPortMessage extends BrokerEpochMessage {
  type: "attach-follower-port";
  followerTabId: string;
  term: number;
  port: MessagePort;
}

export interface BrowserBrokerUseFollowerPortMessage extends BrokerEpochMessage {
  type: "use-follower-port";
  leaderTabId: string;
  term: number;
  port: MessagePort;
}

export interface BrowserBrokerFollowerReadyMessage extends BrokerEpochMessage {
  type: "follower-ready";
  leaderTabId: string;
  term: number;
}

export interface BrowserBrokerCloseFollowerPortMessage extends BrokerEpochMessage {
  type: "close-follower-port";
  term: number;
}

export interface BrowserBrokerStorageResetBeginMessage extends BrokerEpochMessage {
  type: "storage-reset-begin";
  requestId: string;
  term: number;
}

export interface BrowserBrokerStorageResetFinishedMessage extends BrokerEpochMessage {
  type: "storage-reset-finished";
  requestId: string;
  success: boolean;
  errorMessage?: string;
}

export interface BrowserBrokerUnsupportedMessage extends BrokerEpochMessage {
  type: "unsupported";
  reason: string;
}

export type BrowserBrokerControlMessage =
  | BrowserBrokerHelloResponse
  | BrowserBrokerPingMessage
  | BrowserBrokerBecomeLeaderMessage
  | BrowserBrokerDemoteMessage
  | BrowserBrokerLeaderReadyAnnouncement
  | BrowserBrokerAttachFollowerPortMessage
  | BrowserBrokerUseFollowerPortMessage
  | BrowserBrokerFollowerReadyMessage
  | BrowserBrokerCloseFollowerPortMessage
  | BrowserBrokerStorageResetBeginMessage
  | BrowserBrokerStorageResetFinishedMessage
  | BrowserBrokerUnsupportedMessage;

export type BrowserBrokerCapabilityGlobal = {
  SharedWorker?: unknown;
  MessageChannel?: unknown;
  navigator?: {
    locks?: unknown;
  };
};

export function formatUnsupportedBrowserBrokerError(missingCapabilities: string[]): string {
  const missing =
    missingCapabilities.length > 0 ? missingCapabilities.join(", ") : "unknown capability";
  return `Jazz persistent browser mode requires SharedWorker, MessageChannel, and Web Locks support. This environment is missing: ${missing}.`;
}

export function detectBrowserBrokerMissingCapabilities(
  globalLike: BrowserBrokerCapabilityGlobal = globalThis as BrowserBrokerCapabilityGlobal,
): string[] {
  const missing: string[] = [];

  if (typeof globalLike.SharedWorker !== "function") {
    missing.push("SharedWorker");
  }
  if (typeof globalLike.MessageChannel !== "function") {
    missing.push("MessageChannel");
  }

  const locks = globalLike.navigator?.locks as { request?: unknown } | undefined;
  if (!locks || typeof locks.request !== "function") {
    missing.push("Web Locks");
  }

  return missing;
}

export function selectLeaderCandidate(
  candidates: readonly BrowserBrokerCandidate[],
): BrowserBrokerCandidate | null {
  const visible = candidates.filter((candidate) => candidate.visibility === "visible");
  const pool = visible.length > 0 ? visible : candidates;
  let selected: BrowserBrokerCandidate | null = null;

  for (const candidate of pool) {
    if (!selected) {
      selected = candidate;
      continue;
    }

    if (candidate.lastVisibleAt > selected.lastVisibleAt) {
      selected = candidate;
      continue;
    }

    if (candidate.lastVisibleAt === selected.lastVisibleAt && candidate.tabId > selected.tabId) {
      selected = candidate;
    }
  }

  return selected;
}

export function createBrowserBrokerFingerprint(input: BrowserBrokerFingerprintInput): string {
  return stableStringify({
    protocolVersion: BROKER_CONTROL_PROTOCOL_VERSION,
    storageFormatVersion: input.storageFormatVersion ?? BROWSER_STORAGE_FORMAT_VERSION,
    appId: input.appId,
    dbName: input.dbName,
    persistentDriverNamespace: input.persistentDriverNamespace,
    env: input.env,
    userBranch: input.userBranch,
    serverUrl: input.serverUrl ?? null,
    schemaHash: input.schemaHash ?? null,
    runtimeSourceIdentity: input.runtimeSourceIdentity,
    authClass: input.authClass,
  });
}

export function createRuntimeSourceIdentity(runtimeSources?: RuntimeSourcesConfig): string {
  if (!runtimeSources) {
    return "default";
  }

  return stableStringify({
    baseUrl: runtimeSources.baseUrl ?? null,
    workerUrl: runtimeSources.workerUrl ?? null,
    wasmUrl: runtimeSources.wasmUrl ?? null,
    wasmModule: runtimeSources.wasmModule ? "custom-module" : null,
    wasmSource: runtimeSources.wasmSource ? "custom-source" : null,
  });
}

export function createAuthCompatibilityClass(input: {
  jwtToken?: string;
  cookieSession?: Session;
  adminSecret?: string;
}): string {
  if (input.adminSecret) {
    return "admin";
  }

  const session = input.cookieSession;
  if (session?.user_id) {
    return `${session.authMode ?? "user"}:${session.user_id}`;
  }

  return input.jwtToken ? "jwt-authenticated" : "anonymous";
}

function stableStringify(value: unknown): string {
  return JSON.stringify(sortForStableStringify(value));
}

function sortForStableStringify(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortForStableStringify);
  }

  if (!value || typeof value !== "object") {
    return value;
  }

  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = sortForStableStringify((value as Record<string, unknown>)[key]);
  }
  return sorted;
}
