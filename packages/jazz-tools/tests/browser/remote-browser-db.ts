import { commands } from "vitest/browser";
import type {
  RemoteBrowserDbCreateInput,
  RemoteBrowserDbWaitForTitleInput,
} from "./remote-db-harness.js";

declare module "vitest/internal/browser" {
  interface BrowserCommands {
    createRemoteBrowserDb: (input: RemoteBrowserDbCreateInput) => Promise<void>;
    waitForRemoteBrowserDbTitle: (
      input: RemoteBrowserDbWaitForTitleInput,
    ) => Promise<Record<string, unknown>[]>;
    closeRemoteBrowserDb: (id: string) => Promise<void>;
  }
}

export function createRemoteBrowserDb(input: RemoteBrowserDbCreateInput): Promise<void> {
  return commands.createRemoteBrowserDb(input);
}

export function waitForRemoteBrowserDbTitle(
  input: RemoteBrowserDbWaitForTitleInput,
): Promise<Record<string, unknown>[]> {
  return commands.waitForRemoteBrowserDbTitle(input);
}

export function closeRemoteBrowserDb(id: string): Promise<void> {
  return commands.closeRemoteBrowserDb(id);
}
