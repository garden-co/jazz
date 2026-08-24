import { commands } from "vitest/browser";

export interface JazzServerInfo {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export interface JazzServerTopologyInfo {
  topologyId: string;
  appId: string;
  adminSecret: string;
  coreUrl: string;
  edgeUrl: string;
  peerEdgeUrl: string;
}

export interface JazzServerNetworkDebugState {
  contextId: number;
  pattern: string;
  blocked: boolean;
  activePatterns: string[];
}

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    jazzServerInfo: (appId?: string, schema?: number[]) => Promise<JazzServerInfo>;
    jazzServerTopologyInfo: (appId?: string, schema?: number[]) => Promise<JazzServerTopologyInfo>;
    jazzServerTopologyRestartEdge: (
      topologyId: string,
      edgeName: "edge" | "peerEdge",
    ) => Promise<JazzServerTopologyInfo>;
    jazzServerBlockNetwork: (serverUrl: string) => Promise<void>;
    jazzServerUnblockNetwork: (serverUrl: string) => Promise<void>;
    jazzServerJwtForUser: (
      userId: string,
      claims?: Record<string, unknown>,
      appId?: string,
    ) => Promise<string>;
  }
}

export function getJazzServerInfo(appId?: string, schema?: Uint8Array): Promise<JazzServerInfo> {
  return commands.jazzServerInfo(appId, schema ? [...schema] : undefined);
}

export function getJazzServerTopologyInfo(
  appId?: string,
  schema?: Uint8Array,
): Promise<JazzServerTopologyInfo> {
  return commands.jazzServerTopologyInfo(appId, schema ? [...schema] : undefined);
}

export function restartJazzServerTopologyEdge(
  topologyId: string,
  edgeName: "edge" | "peerEdge",
): Promise<JazzServerTopologyInfo> {
  return commands.jazzServerTopologyRestartEdge(topologyId, edgeName);
}

export function blockJazzServerNetwork(serverUrl: string): Promise<void> {
  return commands.jazzServerBlockNetwork(serverUrl);
}

export function unblockJazzServerNetwork(serverUrl: string): Promise<void> {
  return commands.jazzServerUnblockNetwork(serverUrl);
}

export async function getJazzServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
  appId?: string,
): Promise<string> {
  return commands.jazzServerJwtForUser(userId, claims, appId);
}
