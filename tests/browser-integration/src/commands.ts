/// <reference types="@vitest/browser/providers/playwright" />

/**
 * Vitest commands that we can run on the server side to generate chaos and spawn sync servers
 * https://vitest.dev/guide/browser/commands.html
 */

import type { BrowserCommand } from "vitest/node";
import { TestSyncServer, startSyncServer } from "./syncServer";

const syncServers = new Map<string, TestSyncServer>();

const startSyncServerCommand: BrowserCommand<
  [port?: number, dbName?: string]
> = async (ctx, port = 0, dbName) => {
  const syncServer = await startSyncServer(port, dbName);

  syncServers.set(syncServer.url, syncServer);

  return {
    url: syncServer.url,
    port: syncServer.port,
  };
};

const disconnectAllClientsCommand: BrowserCommand<[url: string]> = async (
  ctx,
  url,
) => {
  const syncServer = syncServers.get(url);

  if (!syncServer) {
    throw new Error(`Sync server not found for url: ${url}`);
  }

  syncServer.disconnectAllClients();
};

const setOfflineCommand: BrowserCommand<
  [url: string, active: boolean]
> = async (ctx, url, active) => {
  const syncServer = syncServers.get(url);

  if (!syncServer) {
    throw new Error(`Sync server not found for url: ${url}`);
  }

  syncServer.setActive(active);
};

const closeSyncServerCommand: BrowserCommand<[url: string]> = async (
  ctx,
  url,
) => {
  const syncServer = syncServers.get(url);

  if (!syncServer) {
    throw new Error(`Sync server not found for url: ${url}`);
  }

  syncServer.close();
};

const cleanupCommand: BrowserCommand<[]> = async () => {
  for (const syncServer of syncServers.values()) {
    await syncServer.deleteDb();
  }
};

declare module "@vitest/browser/context" {
  interface BrowserCommands {
    startSyncServer: (
      port?: number,
      dbName?: string,
    ) => Promise<{
      url: `ws://localhost:${number}`;
      port: number;
    }>;
    disconnectAllClients: (url: string) => Promise<void>;
    setOffline: (url: string, active: boolean) => Promise<void>;
    closeSyncServer: (url: string) => Promise<void>;
    cleanup: () => Promise<void>;
  }
}

export const customCommands = {
  startSyncServer: startSyncServerCommand,
  disconnectAllClients: disconnectAllClientsCommand,
  setOffline: setOfflineCommand,
  closeSyncServer: closeSyncServerCommand,
  cleanup: cleanupCommand,
};
