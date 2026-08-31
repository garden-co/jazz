import type { WasmSchema } from "../../drivers/types.js";
import type { RuntimeSourcesConfig } from "../context.js";
import type { MutationErrorEvent } from "../client.js";
import type { NativeSelfSignedClientProof } from "./native-codec.js";

/**
 * Structured-clone-safe Error representation used across browser worker
 * MessagePorts. Error's own fields are not consistently enumerable or retained
 * by every browser, so the relay pins them explicitly.
 */
export interface BrowserRelayError {
  name: string;
  message: string;
  stack?: string;
  /** Stable causal code when the originating error exposes a bounded string code. */
  code?: string;
  cause?: unknown;
}

/** The relay error envelope is deliberately small enough for a MessagePort. */
export const BROWSER_RELAY_ERROR_MAX_CAUSE_DEPTH = 32;
export const BROWSER_RELAY_ERROR_MAX_TOTAL_CHARS = 64 * 1024;
export const BROWSER_RELAY_ERROR_MAX_CODE_CHARS = 128;

const BROWSER_RELAY_ERROR_MAX_NAME_CHARS = 256;
const BROWSER_RELAY_ERROR_MAX_MESSAGE_CHARS = 8 * 1024;
const BROWSER_RELAY_ERROR_MAX_STACK_CHARS = 16 * 1024;
const BROWSER_RELAY_ERROR_MAX_OPAQUE_CAUSE_CHARS = 2 * 1024;
const BROWSER_RELAY_ERROR_PROTOCOL_VIOLATION = "browser_relay_error_protocol_violation";

export function serializeBrowserRelayError(error: unknown): BrowserRelayError {
  const budget = { remaining: BROWSER_RELAY_ERROR_MAX_TOTAL_CHARS };
  const root = serializeBrowserRelayErrorNode(error, budget);
  let source: unknown = error;
  let target = root;
  const seen = new WeakSet<object>();

  for (let depth = 0; depth < BROWSER_RELAY_ERROR_MAX_CAUSE_DEPTH; depth += 1) {
    if (source && typeof source === "object") {
      if (seen.has(source)) break;
      seen.add(source);
    }
    const cause = readErrorProperty(source, "cause");
    if (cause === undefined) break;
    if (!isBrowserRelayErrorLike(cause)) {
      const opaqueCause = serializeOpaqueBrowserRelayCause(cause, budget);
      if (opaqueCause !== undefined) target.cause = opaqueCause;
      break;
    }
    if (depth + 1 > BROWSER_RELAY_ERROR_MAX_CAUSE_DEPTH || budget.remaining === 0) break;
    if (seen.has(cause)) break;
    const nested = serializeBrowserRelayErrorNode(cause, budget);
    target.cause = nested;
    target = nested;
    source = cause;
  }
  return root;
}

export function deserializeBrowserRelayError(serialized: BrowserRelayError): Error {
  const budget = { remaining: BROWSER_RELAY_ERROR_MAX_TOTAL_CHARS };
  const root = deserializeBrowserRelayErrorNode(serialized, budget);
  if (root === null) return browserRelayErrorProtocolViolation();

  let source: unknown = readErrorProperty(serialized, "cause");
  let target = root;
  const seen = new WeakSet<object>();
  if (serialized && typeof serialized === "object") seen.add(serialized);

  for (let depth = 0; source !== undefined; depth += 1) {
    if (depth >= BROWSER_RELAY_ERROR_MAX_CAUSE_DEPTH) return browserRelayErrorProtocolViolation();
    if (!isBrowserRelayErrorLike(source)) {
      const opaqueCause = deserializeOpaqueBrowserRelayCause(source, budget);
      if (opaqueCause === INVALID_BROWSER_RELAY_CAUSE) return browserRelayErrorProtocolViolation();
      attachBrowserRelayCause(target, opaqueCause);
      break;
    }
    if (seen.has(source)) return browserRelayErrorProtocolViolation();
    seen.add(source);
    const nested = deserializeBrowserRelayErrorNode(source, budget);
    if (nested === null) return browserRelayErrorProtocolViolation();
    attachBrowserRelayCause(target, nested);
    target = nested;
    source = readErrorProperty(source, "cause");
  }
  return root;
}

function serializeBrowserRelayErrorNode(
  value: unknown,
  budget: { remaining: number },
): BrowserRelayError {
  const stack = readErrorString(value, "stack");
  const code = validBrowserRelayCode(readErrorProperty(value, "code"));
  const serialized: BrowserRelayError = {
    name: consumeBrowserRelayString(
      readErrorString(value, "name") ?? "Error",
      BROWSER_RELAY_ERROR_MAX_NAME_CHARS,
      budget,
    ),
    message: consumeBrowserRelayString(
      readErrorString(value, "message") ?? safeErrorMessage(value),
      BROWSER_RELAY_ERROR_MAX_MESSAGE_CHARS,
      budget,
    ),
    ...(stack === undefined
      ? {}
      : {
          stack: consumeBrowserRelayString(stack, BROWSER_RELAY_ERROR_MAX_STACK_CHARS, budget),
        }),
  };
  // Codes are compatibility values, never diagnostic fragments. Decide only
  // after consuming earlier fields: otherwise a later code can be sliced to
  // the empty string and make our own envelope fail inbound validation.
  if (code !== undefined && code.length <= budget.remaining) {
    serialized.code = consumeBrowserRelayString(code, BROWSER_RELAY_ERROR_MAX_CODE_CHARS, budget);
  }
  return serialized;
}

function deserializeBrowserRelayErrorNode(
  value: unknown,
  budget: { remaining: number },
): Error | null {
  if (!isBrowserRelayErrorLike(value)) return null;
  const name = readErrorProperty(value, "name");
  const message = readErrorProperty(value, "message");
  const stack = readErrorProperty(value, "stack");
  const code = readErrorProperty(value, "code");
  const validCode = validBrowserRelayCode(code);
  if (
    typeof name !== "string" ||
    typeof message !== "string" ||
    (stack !== undefined && typeof stack !== "string") ||
    (code !== undefined && validCode === undefined) ||
    name.length > BROWSER_RELAY_ERROR_MAX_NAME_CHARS ||
    message.length > BROWSER_RELAY_ERROR_MAX_MESSAGE_CHARS ||
    (typeof stack === "string" && stack.length > BROWSER_RELAY_ERROR_MAX_STACK_CHARS) ||
    name.length +
      message.length +
      (typeof stack === "string" ? stack.length : 0) +
      (typeof code === "string" ? code.length : 0) >
      budget.remaining
  ) {
    return null;
  }
  const error = new Error(
    consumeBrowserRelayString(message, BROWSER_RELAY_ERROR_MAX_MESSAGE_CHARS, budget),
  );
  error.name = consumeBrowserRelayString(name, BROWSER_RELAY_ERROR_MAX_NAME_CHARS, budget);
  if (stack !== undefined) {
    error.stack = consumeBrowserRelayString(stack, BROWSER_RELAY_ERROR_MAX_STACK_CHARS, budget);
  }
  if (validCode !== undefined) {
    Object.defineProperty(error, "code", {
      configurable: true,
      enumerable: true,
      value: consumeBrowserRelayString(validCode, BROWSER_RELAY_ERROR_MAX_CODE_CHARS, budget),
      writable: true,
    });
  }
  return error;
}

function isBrowserRelayErrorLike(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    typeof readErrorProperty(value, "message") === "string"
  );
}

function validBrowserRelayCode(value: unknown): string | undefined {
  return typeof value === "string" &&
    value.length > 0 &&
    value.length <= BROWSER_RELAY_ERROR_MAX_CODE_CHARS
    ? value
    : undefined;
}

function readErrorProperty(value: unknown, property: string): unknown {
  if (value === null || (typeof value !== "object" && typeof value !== "function"))
    return undefined;
  try {
    return (value as Record<string, unknown>)[property];
  } catch {
    return undefined;
  }
}

function readErrorString(value: unknown, property: string): string | undefined {
  const candidate = readErrorProperty(value, property);
  return typeof candidate === "string" ? candidate : undefined;
}

function safeErrorMessage(value: unknown): string {
  try {
    return String(value);
  } catch {
    return "Unstringifiable relay error";
  }
}

function consumeBrowserRelayString(
  value: string,
  perFieldLimit: number,
  budget: { remaining: number },
): string {
  const allowed = Math.min(value.length, perFieldLimit, budget.remaining);
  budget.remaining -= allowed;
  return value.slice(0, allowed);
}

function serializeOpaqueBrowserRelayCause(
  cause: unknown,
  budget: { remaining: number },
): string | number | boolean | null | undefined {
  if (cause === null || typeof cause === "boolean" || typeof cause === "number") return cause;
  if (typeof cause === "string") {
    return consumeBrowserRelayString(cause, BROWSER_RELAY_ERROR_MAX_OPAQUE_CAUSE_CHARS, budget);
  }
  if (cause === undefined) return undefined;
  return consumeBrowserRelayString(
    "[non-error relay cause omitted]",
    BROWSER_RELAY_ERROR_MAX_OPAQUE_CAUSE_CHARS,
    budget,
  );
}

const INVALID_BROWSER_RELAY_CAUSE = Symbol("invalid browser relay cause");

function deserializeOpaqueBrowserRelayCause(
  cause: unknown,
  budget: { remaining: number },
): string | number | boolean | null | undefined | typeof INVALID_BROWSER_RELAY_CAUSE {
  if (cause === null || typeof cause === "boolean" || typeof cause === "number") return cause;
  if (typeof cause === "string") {
    if (
      cause.length > BROWSER_RELAY_ERROR_MAX_OPAQUE_CAUSE_CHARS ||
      cause.length > budget.remaining
    ) {
      return INVALID_BROWSER_RELAY_CAUSE;
    }
    return consumeBrowserRelayString(cause, BROWSER_RELAY_ERROR_MAX_OPAQUE_CAUSE_CHARS, budget);
  }
  return cause === undefined ? undefined : INVALID_BROWSER_RELAY_CAUSE;
}

function attachBrowserRelayCause(error: Error, cause: unknown): void {
  if (cause === undefined) return;
  Object.defineProperty(error, "cause", {
    configurable: true,
    enumerable: false,
    value: cause,
    writable: true,
  });
}

function browserRelayErrorProtocolViolation(): Error {
  const error = new Error("Invalid browser relay error payload");
  error.name = "BrowserRelayErrorProtocolError";
  Object.defineProperty(error, "code", {
    configurable: true,
    enumerable: true,
    value: BROWSER_RELAY_ERROR_PROTOCOL_VIOLATION,
    writable: true,
  });
  return error;
}

export interface BrowserWorkerInitOptions {
  runtimeSources?: RuntimeSourcesConfig;
  schema: WasmSchema;
  dbName: string;
  author: Uint8Array;
  selfSignedClientProof?: NativeSelfSignedClientProof;
  initialSyncFlushEvery: number;
  appId: string;
  /** Non-secret logical owner pinned to an explicitly selected physical IDB root. */
  storageOwner: string;
  authSessionKey: string;
  serverUrl?: string;
  authJson: string;
  sessionClaims: Record<string, unknown>;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
}

export interface BrowserSharedWorkerConnectRequest {
  type: "connect-runtime";
  tabId: string;
  fingerprint: string;
  options: BrowserWorkerInitOptions;
}

/**
 * Liveness handshake sent before a foreground lease request. The worker must
 * acknowledge this without touching durable state; only then may the client
 * send the allocation request on the same port.
 */
export interface BrowserForegroundNodeLeaseProbeRequest {
  type: "probe-foreground-node-lease-worker";
  attemptId: string;
}

/** Lease-only bootstrap that runs before the foreground schema is known. */
export interface BrowserForegroundNodeLeaseAcquireRequest {
  type: "acquire-foreground-node-lease";
  /** Correlates this durable operation with its preceding liveness probe. */
  attemptId?: string;
  dbName: string;
  /** Exact durable owner that must admit the physical root before lease issue. */
  storageOwner: string;
  /** @internal Browser-only scheduling seam for the real worker receipt. */
  testDelayBeforeLeaseAllocationMs?: number;
  /** @internal Browser-only scheduling seam for the real worker receipt. */
  testDelayAfterLeaseAllocationMs?: number;
}

/** Cancel a lease bootstrap that has not been handed to a foreground yet. */
export interface BrowserForegroundNodeLeaseCancelRequest {
  type: "cancel-foreground-node-lease";
}

export type BrowserForegroundNodeLeaseAcquireResponse =
  | {
      type: "foreground-node-lease-worker-alive";
      attemptId: string;
    }
  | {
      /** The named realm accepted termination and must not admit durable work. */
      type: "foreground-node-lease-worker-closing";
      attemptId: string;
    }
  /** A different realm still owns the physical root; retrying is safe. */
  | { type: "foreground-node-lease-busy"; message: string }
  | {
      type: "foreground-node-lease-ready";
      leaseId: string;
      node: Uint8Array;
      /** Canonical decimal u64: never a lossy JS number. */
      confirmedTxTime: string;
    }
  /** @internal Emitted only by a capability-bearing browser test worker. */
  | {
      type: "foreground-node-lease-test-allocated";
      node: Uint8Array;
      /** @internal Test-worker realm marker, never shipped in production. */
      workerRealmId: string;
    }
  | { type: "foreground-node-lease-error"; error: BrowserRelayError }
  /**
   * The worker observed cancellation and either had no lease to clean up or
   * durably retired the lease that finished concurrently with cancellation.
   */
  | {
      type: "foreground-node-lease-cancelled";
      error?: BrowserRelayError;
      /** @internal Test-only durable state; absent from ordinary worker replies. */
      testLeaseState?: "active" | "reusable" | "retired" | "missing";
    };

export type BrowserForegroundNodeLeasePortRequest =
  | { type: "return-foreground-node-lease"; confirmedTxTime: string }
  | { type: "retire-foreground-node-lease" };

export type BrowserForegroundNodeLeasePortEvent = {
  type: "foreground-node-lease-result";
  error?: BrowserRelayError;
};

export type BrowserSharedWorkerConnectResponse =
  | { type: "worker-alive" }
  | { type: "runtime-ready" }
  | { type: "runtime-error"; error: BrowserRelayError }
  /** The realm has acknowledged inspector-directed termination. */
  | { type: "worker-closing" };

export type BrowserFollowerPortRequest =
  | { type: "init"; id: number; sessionClaims: Record<string, unknown> }
  | { type: "frames"; frames: Uint8Array[] }
  /** @internal Trace-only redacted query-coverage progress from a foreground tab. */
  | {
      type: "diagnostic-query-coverage";
      stage: "attach" | "covered";
      peerActivityEpoch: number;
      peerProcessedActivityEpoch: number;
    }
  | { type: "update-auth"; authJson: string; sessionClaims: Record<string, unknown> }
  | { type: "wait-server"; id: number }
  | { type: "disconnect"; id: number }
  | { type: "flush-local"; id: number }
  | { type: "flush-local-observed" }
  | { type: "prepare-storage-reset"; id: number }
  | { type: "finish-storage-reset"; id: number }
  | { type: "abort-storage-reset"; id: number }
  | { type: "storage-reset-observed"; resetId: number }
  | { type: "open-inspector-control"; id: number; port: MessagePort }
  | {
      type: "reconnect";
      id: number;
      authJson: string;
      sessionClaims: Record<string, unknown>;
    }
  | { type: "close"; id?: number; releaseContext?: boolean };

export interface BrowserInspectorContext {
  workerRealmId: string;
  key: string;
  appId: string;
  dbName: string;
  schema: WasmSchema;
}

/**
 * Bounded, redacted lifecycle evidence for diagnosing browser-worker stalls.
 * It deliberately carries no query text, row data, credentials, or wire
 * frames: ordering and counters are enough to establish handoff progress.
 */
export type BrowserWorkerLifecycleTrace = {
  sequence: number;
  event:
    | "bootstrap-start"
    | "lease-request"
    | "lease-admitted"
    | "peer-attached"
    | "peer-frames"
    | "query-attach"
    | "query-covered"
    | "owner-release-start"
    | "owner-release-finished";
  dbName: string;
  peerCount: number;
  pendingBootstraps: number;
  activeLeases: number;
  frameCount?: number;
  peerActivityEpoch?: number;
  peerProcessedActivityEpoch?: number;
};

/**
 * Redacted flight-recorder entry for a browser chunk relay. Hashes and
 * locators are short fingerprints; no retrieval capability is exposed.
 */
export type BrowserRelayTrace = {
  hop: "tab-worker" | "worker-tab" | "worker-server";
  event: string;
  role: "upstream" | "subscriber";
  connection: string;
  requestId: string;
  remainingHops: number;
  objectHash: string;
  locatorFingerprint: string;
  response?: "found" | "unavailable" | "retryable";
  storageError?: "unavailable" | "locator-conflict" | "integrity" | "backend";
};

export type BrowserInspectorControlRequest =
  | { type: "list-contexts"; id: number }
  | { type: "lifecycle-trace"; id: number }
  | { type: "terminate-worker"; id: number }
  | {
      type: "attach-context";
      id: number;
      contextKey: string;
      tabId: string;
      port: MessagePort;
    }
  | { type: "close" };

export type BrowserInspectorControlEvent =
  | { type: "contexts"; id: number; contexts: BrowserInspectorContext[] }
  | { type: "lifecycle-trace"; id: number; entries: BrowserWorkerLifecycleTrace[] }
  | {
      type: "result";
      id: number;
      error?: BrowserRelayError;
      /** The acknowledged realm is closing; future opens must use its successor. */
      workerTerminated?: true;
    };

export type BrowserFollowerPortEvent =
  | { type: "frames"; frames: Uint8Array[] }
  /**
   * Explicit offline is a property of the durable SharedWorker namespace: it
   * owns the one upstream server connection shared by all attached tabs.
   */
  | { type: "transport-state"; explicitlyDisconnected: boolean }
  | { type: "result"; id: number; error?: BrowserRelayError }
  | { type: "auth-failure"; reason: string }
  | { type: "auth-restored" }
  | { type: "mutation-error"; event: MutationErrorEvent }
  /**
   * A terminal failure of the worker-owned upstream server transport. This is
   * deliberately not a mutation rejection: the follower records it only to
   * reject its active remote waits and subscriptions.
   */
  | { type: "transport-error"; error: BrowserRelayError }
  | { type: "storage-reset"; resetId: number }
  | { type: "storage-invalidated" }
  | { type: "relay-trace"; entries: BrowserRelayTrace[] }
  | { type: "error"; error: BrowserRelayError };
