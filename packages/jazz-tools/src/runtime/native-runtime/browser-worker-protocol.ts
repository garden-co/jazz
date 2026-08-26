import type { WasmSchema } from "../../drivers/types.js";
import type { RuntimeSourcesConfig } from "../context.js";
import type { MutationErrorEvent } from "../client.js";
import type { NativeSelfSignedClientProof } from "./native-codec.js";

export interface BrowserWorkerInitOptions {
  runtimeSources?: RuntimeSourcesConfig;
  schema: WasmSchema;
  dbName: string;
  node: Uint8Array;
  author: Uint8Array;
  selfSignedClientProof?: NativeSelfSignedClientProof;
  initialSyncFlushEvery: number;
  appId: string;
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

export type BrowserSharedWorkerConnectResponse =
  | { type: "worker-alive" }
  | { type: "runtime-ready" }
  | { type: "runtime-error"; message: string };

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
  | { type: "result"; id: number; error?: string }
  | { type: "auth-failure"; reason: string }
  | { type: "auth-restored" }
  | { type: "mutation-error"; event: MutationErrorEvent }
  | { type: "storage-reset"; resetId: number }
  | { type: "storage-invalidated" }
  | { type: "relay-trace"; entries: BrowserRelayTrace[] }
  | { type: "error"; message: string };
