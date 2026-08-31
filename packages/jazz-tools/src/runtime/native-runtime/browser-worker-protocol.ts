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
  cause?: unknown;
}

export function serializeBrowserRelayError(error: unknown): BrowserRelayError {
  return serializeBrowserRelayErrorWithSeen(error, new WeakSet<object>());
}

export function deserializeBrowserRelayError(serialized: BrowserRelayError): Error {
  const cause = deserializeBrowserRelayCause(serialized.cause);
  const error = new Error(serialized.message);
  error.name = serialized.name;
  if (serialized.stack !== undefined) error.stack = serialized.stack;
  if (cause !== undefined) {
    Object.defineProperty(error, "cause", {
      configurable: true,
      enumerable: false,
      value: cause,
      writable: true,
    });
  }
  return error;
}

function serializeBrowserRelayErrorWithSeen(
  value: unknown,
  seen: WeakSet<object>,
): BrowserRelayError {
  const error = browserRelayErrorLike(value);
  if (typeof value === "object" && value !== null) {
    if (seen.has(value)) {
      return {
        name: error.name,
        message: error.message,
        ...(error.stack === undefined ? {} : { stack: error.stack }),
      };
    }
    seen.add(value);
  }
  const serialized: BrowserRelayError = {
    name: error.name,
    message: error.message,
    ...(error.stack === undefined ? {} : { stack: error.stack }),
  };
  if (error.cause !== undefined) {
    serialized.cause = serializeBrowserRelayCause(error.cause, seen);
  }
  return serialized;
}

function browserRelayErrorLike(value: unknown): {
  name: string;
  message: string;
  stack?: string;
  cause?: unknown;
} {
  if (value && typeof value === "object" && "message" in value) {
    const message = typeof value.message === "string" ? value.message : String(value.message);
    const name = "name" in value && typeof value.name === "string" ? value.name : "Error";
    const stack = "stack" in value && typeof value.stack === "string" ? value.stack : undefined;
    const cause = "cause" in value ? value.cause : undefined;
    return {
      name,
      message,
      ...(stack === undefined ? {} : { stack }),
      ...(cause === undefined ? {} : { cause }),
    };
  }
  return { name: "Error", message: String(value) };
}

function serializeBrowserRelayCause(cause: unknown, seen: WeakSet<object>): unknown {
  if (cause && typeof cause === "object" && "message" in cause) {
    return serializeBrowserRelayErrorWithSeen(cause, seen);
  }
  try {
    return structuredClone(cause);
  } catch {
    return String(cause);
  }
}

function deserializeBrowserRelayCause(cause: unknown): unknown {
  if (
    cause &&
    typeof cause === "object" &&
    "name" in cause &&
    typeof cause.name === "string" &&
    "message" in cause &&
    typeof cause.message === "string"
  ) {
    const serialized: BrowserRelayError = {
      name: cause.name,
      message: cause.message,
      ...("stack" in cause && typeof cause.stack === "string" ? { stack: cause.stack } : {}),
      ...("cause" in cause ? { cause: cause.cause } : {}),
    };
    return deserializeBrowserRelayError(serialized);
  }
  return cause;
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

/** Lease-only bootstrap that runs before the foreground schema is known. */
export interface BrowserForegroundNodeLeaseAcquireRequest {
  type: "acquire-foreground-node-lease";
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
  | { type: "runtime-error"; error: BrowserRelayError };

export type BrowserFollowerPortRequest =
  | { type: "init"; id: number; sessionClaims: Record<string, unknown> }
  | { type: "frames"; frames: Uint8Array[] }
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
  | { type: "result"; id: number; error?: string };

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
