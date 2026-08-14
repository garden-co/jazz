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
  sessionClaims: Record<string, unknown>;
  leadershipId: number;
  workerLockName: string;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
}

export type BrowserWorkerRequest =
  | ({ type: "init" } & BrowserWorkerInitOptions)
  | { type: "wait-server" }
  | { type: "update-auth"; authJson: string; sessionClaims: Record<string, unknown> }
  | { type: "disconnect" }
  | { type: "reconnect"; authJson: string; sessionClaims: Record<string, unknown> }
  | {
      type: "attach-follower";
      followerTabId: string;
      leadershipId: number;
      port: MessagePort;
    }
  | { type: "detach-follower"; followerTabId: string; leadershipId: number }
  | { type: "delete-storage" }
  | { type: "simulate-crash" }
  | { type: "close" };

export type BrowserWorkerMessage =
  | ({ id: number } & BrowserWorkerRequest)
  | { type: "frames"; frames: Uint8Array[] };

export type BrowserWorkerEvent =
  | { type: "result"; id: number; error?: string }
  | { type: "frames"; frames: Uint8Array[] }
  | { type: "auth-failure"; reason: string }
  | { type: "follower-port-closed"; followerTabId: string; leadershipId: number }
  | { type: "error"; message: string };

export type BrowserFollowerPortRequest =
  | { type: "init"; id: number; sessionClaims: Record<string, unknown> }
  | { type: "frames"; frames: Uint8Array[] }
  | { type: "update-auth"; authJson: string; sessionClaims: Record<string, unknown> }
  | { type: "wait-server"; id: number }
  | { type: "close" };

export type BrowserFollowerPortEvent =
  | { type: "frames"; frames: Uint8Array[] }
  | { type: "result"; id: number; error?: string }
  | { type: "auth-failure"; reason: string }
  | { type: "error"; message: string };
