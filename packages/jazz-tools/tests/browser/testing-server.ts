import { commands } from "vitest/browser";

export interface TestingServerInfo {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    testingServerInfo: () => Promise<TestingServerInfo>;
    testingServerJwtForUser: (userId: string, claims?: Record<string, unknown>) => Promise<string>;
  }
}

export function getTestingServerInfo(): Promise<TestingServerInfo> {
  return commands.testingServerInfo();
}

export async function getTestingServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
): Promise<string> {
  return commands.testingServerJwtForUser(userId, claims);
}
