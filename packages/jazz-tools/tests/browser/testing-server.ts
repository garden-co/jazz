import { commands } from "vitest/browser";

export interface TestingServerInfo {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export interface TestingServerNetworkDebugState {
  contextId: number;
  pattern: string;
  blocked: boolean;
  activePatterns: string[];
}

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    testingServerInfo: () => Promise<TestingServerInfo>;
    testingServerBlockNetwork: (serverUrl: string) => Promise<void>;
    testingServerUnblockNetwork: (serverUrl: string) => Promise<void>;
    testingServerNetworkDebug: (serverUrl: string) => Promise<TestingServerNetworkDebugState>;
    testingServerJwtForUser: (userId: string, claims?: Record<string, unknown>) => Promise<string>;
  }
}

export function getTestingServerInfo(): Promise<TestingServerInfo> {
  return commands.testingServerInfo();
}

export function blockTestingServerNetwork(serverUrl: string): Promise<void> {
  return commands.testingServerBlockNetwork(serverUrl);
}

export function unblockTestingServerNetwork(serverUrl: string): Promise<void> {
  return commands.testingServerUnblockNetwork(serverUrl);
}

export function getTestingServerNetworkDebug(
  serverUrl: string,
): Promise<TestingServerNetworkDebugState> {
  return commands.testingServerNetworkDebug(serverUrl);
}

export async function getTestingServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
): Promise<string> {
  return commands.testingServerJwtForUser(userId, claims);
}
