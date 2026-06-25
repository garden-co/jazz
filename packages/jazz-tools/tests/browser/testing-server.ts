import { commands } from "vitest/browser";

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

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    jazzServerInfo: (appId?: string) => Promise<JazzServerInfo>;
    jazzServerBlockNetwork: (serverUrl: string) => Promise<void>;
    jazzServerUnblockNetwork: (serverUrl: string) => Promise<void>;
    jazzServerJwtForUser: (
      userId: string,
      claims?: Record<string, unknown>,
      appId?: string,
    ) => Promise<string>;
  }
}

export function getJazzServerInfo(appId?: string): Promise<JazzServerInfo> {
  return commands.jazzServerInfo(appId);
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
