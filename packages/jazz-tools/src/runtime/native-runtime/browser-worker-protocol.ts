import type { WasmSchema } from "../../drivers/types.js";
import type { RuntimeSourcesConfig } from "../context.js";

export interface BrowserWorkerInitOptions {
  runtimeSources?: RuntimeSourcesConfig;
  schema: WasmSchema;
  dbName: string;
  node: Uint8Array;
  author: Uint8Array;
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
  | { type: "runtime-ready" }
  | { type: "runtime-error"; message: string };

export type BrowserFollowerPortRequest =
  | { type: "init"; id: number; sessionClaims: Record<string, unknown> }
  | { type: "frames"; frames: Uint8Array[] }
  | { type: "update-auth"; authJson: string; sessionClaims: Record<string, unknown> }
  | { type: "wait-server"; id: number }
  | { type: "disconnect"; id: number }
  | { type: "delete-storage"; id: number }
  | { type: "storage-reset-observed"; resetId: number }
  | { type: "open-inspector-control"; id: number; port: MessagePort }
  | {
      type: "reconnect";
      id: number;
      authJson: string;
      sessionClaims: Record<string, unknown>;
    }
  | { type: "close" };

export interface BrowserInspectorContext {
  key: string;
  appId: string;
  dbName: string;
  schema: WasmSchema;
}

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
  | { type: "storage-reset"; resetId: number }
  | { type: "storage-invalidated" }
  | { type: "error"; message: string };
