/**
 * Wire protocol for the Safari SharedWorker leader — control plane only.
 *
 * Data-plane payloads on the leader-minted MessagePort use plain JS objects:
 *   { type: "follower-sync", payload: Uint8Array[] }   // tab -> leader
 *   { type: "leader-sync",   payload: Uint8Array[] }   // leader -> tab
 *
 * One round-trip handshake:
 *   tab -> leader: CONNECT { everything the leader needs to bootstrap + identify the tab }
 *   leader -> tab: PEER_PORT { port, generation }  OR  LEADER_FAULT { reason }
 */

export const LEADER_PROTOCOL_VERSION = 1;

export function buildLeaderScope(appId: string, dbName: string): string {
  return `${appId}::${dbName}`;
}

export function buildLeaderWorkerName(scope: string): string {
  return `jazz-shared-worker-leader:${scope}`;
}

export function buildLockName(appId: string, dbName: string): string {
  return `jazz-worker:${appId}:${dbName}`;
}

export type TabId = string;

export interface ConnectMessage {
  t: "CONNECT";
  tabId: TabId;
  bornAt: number;
  scope: string;
  protocolVersion: number;
  jazzPackageVersion: string;
  appId: string;
  dbName: string;
  schemaJson: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
}

export type TabToLeader = ConnectMessage | { t: "CHECK_CAPABILITY" } | { t: "GOODBYE" };

export type LeaderFaultReason =
  | "version-mismatch"
  | "scope-mismatch"
  | "runtime-host-unavailable"
  | "init-failed";

export type LeaderToTab =
  | { t: "PEER_PORT"; port: MessagePort; generation: number }
  | { t: "CAPABILITY_RESULT"; supported: boolean }
  | { t: "LEADER_FAULT"; reason: LeaderFaultReason; detail?: string };

export function isTabToLeader(value: unknown): value is TabToLeader {
  if (typeof value !== "object" || value === null) return false;
  const m = value as { t?: unknown };
  switch (m.t) {
    case "CONNECT": {
      const h = value as Record<string, unknown>;
      return (
        typeof h.tabId === "string" &&
        typeof h.bornAt === "number" &&
        typeof h.scope === "string" &&
        typeof h.protocolVersion === "number" &&
        typeof h.jazzPackageVersion === "string" &&
        typeof h.appId === "string" &&
        typeof h.dbName === "string" &&
        typeof h.schemaJson === "string"
      );
    }
    case "CHECK_CAPABILITY":
    case "GOODBYE":
      return true;
    default:
      return false;
  }
}

export function isLeaderToTab(value: unknown): value is LeaderToTab {
  if (typeof value !== "object" || value === null) return false;
  const m = value as { t?: unknown };
  switch (m.t) {
    case "PEER_PORT": {
      const m2 = value as Record<string, unknown>;
      return typeof m2.generation === "number" && m2.port instanceof MessagePort;
    }
    case "CAPABILITY_RESULT": {
      return typeof (value as { supported?: unknown }).supported === "boolean";
    }
    case "LEADER_FAULT": {
      const m2 = value as Record<string, unknown>;
      return typeof m2.reason === "string";
    }
    default:
      return false;
  }
}
