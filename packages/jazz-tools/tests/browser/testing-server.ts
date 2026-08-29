import { jazzServerBrowserCommands } from "./browser-commands.js";

export interface JazzServerInfo {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export interface JazzServerNetworkDebugState {
  contextId: number;
  pattern: string;
  blocked: boolean;
  activePatterns: string[];
}

export function getJazzServerInfo(appId?: string, schema?: Uint8Array): Promise<JazzServerInfo> {
  return jazzServerBrowserCommands().jazzServerInfo(appId, schema ? [...schema] : undefined);
}

export function stopJazzServer(serverUrl: string): Promise<void> {
  return jazzServerBrowserCommands().jazzServerStop(serverUrl);
}

export function blockJazzServerNetwork(serverUrl: string): Promise<void> {
  return jazzServerBrowserCommands().jazzServerBlockNetwork(serverUrl);
}

export function unblockJazzServerNetwork(serverUrl: string): Promise<void> {
  return jazzServerBrowserCommands().jazzServerUnblockNetwork(serverUrl);
}

export async function getJazzServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
  appId?: string,
): Promise<string> {
  return jazzServerBrowserCommands().jazzServerJwtForUser(userId, claims, appId);
}
