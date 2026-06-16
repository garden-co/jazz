import type { RuntimeSourcesConfig } from "./context.js";
import type { BrowserBrokerUnsupportedCode } from "./browser-broker-errors.js";

/**
 * Tab<->broker wire-format version. It is embedded in the hello fingerprint,
 * so tabs speaking a different protocol version are rejected with
 * `unsupported` at attach time instead of having their messages silently
 * dropped later. Bump whenever the shape or required fields of any broker
 * message change.
 */
export const BROKER_CONTROL_PROTOCOL_VERSION = "jazz-browser-broker-v3";
export const BROWSER_STORAGE_FORMAT_VERSION = "opfs-btree-v1";

// Liveness defaults shared by the broker worker and the tab client — a drift
// between the two desynchronizes ping cadence from eviction timing.
export const DEFAULT_BROKER_PING_INTERVAL_MS = 1_000;
export const DEFAULT_BROKER_PONG_TIMEOUT_MS = 3_000;

export function normalizePositiveTimeout(value: unknown, fallback: number): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.max(1, Math.floor(value));
}

export function stringifyError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function createRandomId(prefix?: string): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  const raw =
    cryptoObj && typeof cryptoObj.randomUUID === "function"
      ? cryptoObj.randomUUID()
      : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  return prefix ? `${prefix}-${raw}` : raw;
}

export type BrowserBrokerVisibility = "visible" | "hidden";
export type BrowserBrokerRole = "leader" | "follower";

/**
 * Broker protocol naming:
 * - `brokerInstanceId` identifies one in-memory SharedWorker broker instance.
 * - `leadershipId` identifies one leader promotion within that broker instance;
 *   it is separate from `leaderTabId` because the same tab can become leader
 *   multiple times across failovers.
 */

export interface BrokerInstanceMessage {
  brokerInstanceId: string;
}

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

export interface BrowserBrokerVisibilityMessage extends BrokerInstanceMessage {
  type: "visibility";
  visibility: BrowserBrokerVisibility;
}

export interface BrowserBrokerLeaderReadyMessage extends BrokerInstanceMessage {
  type: "leader-ready";
  leadershipId: number;
  tabLockName: string;
  workerLockName: string;
  /**
   * Set by a reset-promoted leader that has no client to rebuild a worker
   * bridge from (fresh namespace). Tells the broker to finish the storage
   * reset and step this leader down instead of treating it as ready.
   */
  bridgelessStorageReset?: boolean;
}

export interface BrowserBrokerLeaderFailedMessage extends BrokerInstanceMessage {
  type: "leader-failed";
  leadershipId: number;
  reason: string;
}

export interface BrowserBrokerFollowerPortAttachedMessage extends BrokerInstanceMessage {
  type: "follower-port-attached";
  leadershipId: number;
  followerTabId: string;
}

export interface BrowserBrokerFollowerPortClosedMessage extends BrokerInstanceMessage {
  type: "follower-port-closed";
  leadershipId: number;
  followerTabId: string;
}

export interface BrowserBrokerSchemaReadyMessage extends BrokerInstanceMessage {
  type: "schema-ready";
  schemaFingerprint: string;
}

export interface BrowserBrokerStorageResetRequestMessage extends BrokerInstanceMessage {
  type: "storage-reset-request";
  requestId: string;
}

export interface BrowserBrokerStorageResetReadyMessage extends BrokerInstanceMessage {
  type: "storage-reset-ready";
  requestId: string;
  success: boolean;
  errorMessage?: string;
}

export interface BrowserBrokerShutdownMessage extends BrokerInstanceMessage {
  type: "shutdown";
}

export interface BrowserBrokerPongMessage extends BrokerInstanceMessage {
  type: "broker-pong";
}

export type BrowserBrokerTabMessage =
  | BrowserBrokerHelloMessage
  | BrowserBrokerVisibilityMessage
  | BrowserBrokerLeaderReadyMessage
  | BrowserBrokerLeaderFailedMessage
  | BrowserBrokerFollowerPortAttachedMessage
  | BrowserBrokerFollowerPortClosedMessage
  | BrowserBrokerSchemaReadyMessage
  | BrowserBrokerStorageResetRequestMessage
  | BrowserBrokerStorageResetReadyMessage
  | BrowserBrokerShutdownMessage
  | BrowserBrokerPongMessage;

type WithoutBrokerInstance<T> = T extends BrokerInstanceMessage ? Omit<T, "brokerInstanceId"> : T;

export type BrowserBrokerTabMessageInput =
  | BrowserBrokerHelloMessage
  | WithoutBrokerInstance<Exclude<BrowserBrokerTabMessage, BrowserBrokerHelloMessage>>;

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

export interface BrowserBrokerDetachFollowerPortMessage extends BrokerInstanceMessage {
  type: "detach-follower-port";
  followerTabId: string;
  leadershipId: number;
}

export interface BrowserBrokerStorageResetBeginMessage extends BrokerInstanceMessage {
  type: "storage-reset-begin";
  requestId: string;
  leadershipId: number;
}

export interface BrowserBrokerStorageResetStartedMessage extends BrokerInstanceMessage {
  type: "storage-reset-started";
  requestId: string;
}

export interface BrowserBrokerStorageResetFinishedMessage extends BrokerInstanceMessage {
  type: "storage-reset-finished";
  requestId: string;
  success: boolean;
  errorMessage?: string;
}

export interface BrowserBrokerUnsupportedMessage extends BrokerInstanceMessage {
  type: "unsupported";
  code?: BrowserBrokerUnsupportedCode;
  reason: string;
}

export interface BrowserBrokerSchemaBlockedMessage extends BrokerInstanceMessage {
  type: "schema-blocked";
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
  | BrowserBrokerDetachFollowerPortMessage
  | BrowserBrokerStorageResetBeginMessage
  | BrowserBrokerStorageResetStartedMessage
  | BrowserBrokerStorageResetFinishedMessage
  | BrowserBrokerUnsupportedMessage
  | BrowserBrokerSchemaBlockedMessage;

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

// Leadership ids increase monotonically per namespace. "Stale" means the
// message belongs to a leadership that has been superseded on this receiver;
// "future" means the receiver has not observed that leadership yet. Exact
// matches are compared with plain equality at the call sites.
export function isStaleLeadershipId(incoming: number, current: number): boolean {
  return incoming < current;
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
    brokerWorkerUrl: runtimeSources.brokerWorkerUrl ?? null,
    wasmUrl: runtimeSources.wasmUrl ?? null,
    wasmModule: runtimeSources.wasmModule
      ? getObjectRuntimeSourceIdentity("wasm-module", runtimeSources.wasmModule)
      : null,
    wasmSource: runtimeSources.wasmSource
      ? getBufferSourceRuntimeSourceIdentity(runtimeSources.wasmSource)
      : null,
  });
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
