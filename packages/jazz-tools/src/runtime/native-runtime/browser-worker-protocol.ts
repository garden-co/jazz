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
  serverUrl?: string;
  authJson: string;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
}

export type BrowserWorkerRequest =
  | ({ type: "init" } & BrowserWorkerInitOptions)
  | { type: "wait-server" }
  | { type: "update-auth"; authJson: string }
  | { type: "disconnect" }
  | { type: "reconnect"; authJson: string }
  | { type: "delete-storage" }
  | { type: "close" };

export type BrowserWorkerMessage =
  | ({ id: number } & BrowserWorkerRequest)
  | { type: "frames"; frames: Uint8Array[] };

export type BrowserWorkerEvent =
  | { type: "result"; id: number; error?: string }
  | { type: "frames"; frames: Uint8Array[] }
  | { type: "auth-failure"; reason: string }
  | { type: "error"; message: string };
