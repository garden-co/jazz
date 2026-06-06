import type { RuntimeSourcesConfig, Session } from "./context.js";

export const BROKER_CONTROL_PROTOCOL_VERSION = "jazz-browser-broker-v1";
export const BROWSER_STORAGE_FORMAT_VERSION = "opfs-btree-v1";

export type BrowserBrokerVisibility = "visible" | "hidden";
export type BrowserBrokerRole = "leader" | "follower";

/**
 * Broker protocol naming:
 * - `brokerInstanceId` identifies one in-memory SharedWorker broker instance.
 * - `leadershipId` identifies one leader promotion within that broker instance;
 *   it is separate from `leaderTabId` because the same tab can become leader
 *   multiple times across failovers.
 */

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
  leadershipId: number;
  tabLockName: string;
  workerLockName: string;
  compatibilityLockName?: string;
}

export interface BrowserBrokerLeaderFailedMessage {
  type: "leader-failed";
  leadershipId: number;
  reason: string;
}

export interface BrowserBrokerFollowerPortAttachedMessage {
  type: "follower-port-attached";
  leadershipId: number;
  followerTabId: string;
}

export interface BrowserBrokerSchemaReadyMessage {
  type: "schema-ready";
  schemaFingerprint: string;
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
  brokerInstanceId: string;
}

export type BrowserBrokerTabMessage =
  | BrowserBrokerHelloMessage
  | BrowserBrokerVisibilityMessage
  | BrowserBrokerLeaderReadyMessage
  | BrowserBrokerLeaderFailedMessage
  | BrowserBrokerFollowerPortAttachedMessage
  | BrowserBrokerSchemaReadyMessage
  | BrowserBrokerStorageResetRequestMessage
  | BrowserBrokerStorageResetReadyMessage
  | BrowserBrokerShutdownMessage
  | BrowserBrokerPongMessage;

export interface BrokerInstanceMessage {
  brokerInstanceId: string;
}

export interface BrowserBrokerHelloResponse extends BrokerInstanceMessage {
  type: "broker-hello";
}

export interface BrowserBrokerPingMessage extends BrokerInstanceMessage {
  type: "broker-ping";
}

export interface BrowserBrokerBecomeLeaderMessage extends BrokerInstanceMessage {
  type: "become-leader";
  leadershipId: number;
  resetRequestId?: string;
}

export interface BrowserBrokerDemoteMessage extends BrokerInstanceMessage {
  type: "demote";
  leadershipId: number;
}

export interface BrowserBrokerLeaderReadyAnnouncement extends BrokerInstanceMessage {
  type: "leader-ready";
  leaderTabId: string;
  leadershipId: number;
}

export interface BrowserBrokerAttachFollowerPortMessage extends BrokerInstanceMessage {
  type: "attach-follower-port";
  followerTabId: string;
  leadershipId: number;
  port: MessagePort;
}

export interface BrowserBrokerUseFollowerPortMessage extends BrokerInstanceMessage {
  type: "use-follower-port";
  leaderTabId: string;
  leadershipId: number;
  port: MessagePort;
}

export interface BrowserBrokerFollowerReadyMessage extends BrokerInstanceMessage {
  type: "follower-ready";
  leaderTabId: string;
  leadershipId: number;
}

export interface BrowserBrokerCloseFollowerPortMessage extends BrokerInstanceMessage {
  type: "close-follower-port";
  leadershipId: number;
}

export interface BrowserBrokerStorageResetBeginMessage extends BrokerInstanceMessage {
  type: "storage-reset-begin";
  requestId: string;
  leadershipId: number;
}

export interface BrowserBrokerStorageResetFinishedMessage extends BrokerInstanceMessage {
  type: "storage-reset-finished";
  requestId: string;
  success: boolean;
  errorMessage?: string;
}

export interface BrowserBrokerUnsupportedMessage extends BrokerInstanceMessage {
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
    wasmModule: runtimeSources.wasmModule
      ? getObjectRuntimeSourceIdentity("wasm-module", runtimeSources.wasmModule)
      : null,
    wasmSource: runtimeSources.wasmSource
      ? getBufferSourceRuntimeSourceIdentity(runtimeSources.wasmSource)
      : null,
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

let runtimeSourceObjectIdentityCounter = 0;
const runtimeSourceObjectIdentities = new WeakMap<object, string>();

function getObjectRuntimeSourceIdentity(prefix: string, value: object): string {
  let identity = runtimeSourceObjectIdentities.get(value);
  if (!identity) {
    runtimeSourceObjectIdentityCounter += 1;
    identity = `${prefix}:${runtimeSourceObjectIdentityCounter}`;
    runtimeSourceObjectIdentities.set(value, identity);
  }
  return identity;
}

function getBufferSourceRuntimeSourceIdentity(value: BufferSource): string {
  try {
    const bytes = bufferSourceBytes(value);
    return `wasm-source:${bytes.byteLength}:${hashBytes(bytes)}`;
  } catch {
    return getObjectRuntimeSourceIdentity("wasm-source", value as object);
  }
}

function bufferSourceBytes(value: BufferSource): Uint8Array {
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return new Uint8Array(value);
}

function hashBytes(bytes: Uint8Array): string {
  let h1 = 0x811c9dc5;
  let h2 = 0x01000193;

  for (const byte of bytes) {
    h1 ^= byte;
    h1 = Math.imul(h1, 0x01000193);
    h2 ^= byte;
    h2 = Math.imul(h2, 0x85ebca6b);
  }

  return `${(h1 >>> 0).toString(16).padStart(8, "0")}${(h2 >>> 0).toString(16).padStart(8, "0")}`;
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
