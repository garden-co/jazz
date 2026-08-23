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
    insertRemoteBrowserDbRow: (
      id: string,
      tabIndex: number,
      row: Record<string, unknown>,
      table?: string,
    ) => Promise<string>;
    updateRemoteBrowserDbRow: (
      id: string,
      tabIndex: number,
      rowId: string,
      patch: Record<string, unknown>,
      table?: string,
    ) => Promise<void>;
    queryRemoteBrowserDbRows: (
      id: string,
      tabIndex: number,
      tier?: "local" | "edge",
    ) => Promise<Record<string, unknown>[]>;
    restartRemoteBrowserDb: (id: string) => Promise<void>;
    deleteRemoteBrowserIndexedDbAndWaitForReload: (id: string, dbName: string) => Promise<void>;
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

export function insertRemoteBrowserDbRow(
  id: string,
  tabIndex: number,
  row: Record<string, unknown>,
  table?: string,
): Promise<string> {
  return commands.insertRemoteBrowserDbRow(id, tabIndex, row, table);
}

export function updateRemoteBrowserDbRow(
  id: string,
  tabIndex: number,
  rowId: string,
  patch: Record<string, unknown>,
  table?: string,
): Promise<void> {
  return commands.updateRemoteBrowserDbRow(id, tabIndex, rowId, patch, table);
}

export function queryRemoteBrowserDbRows(
  id: string,
  tabIndex: number,
  tier?: "local" | "edge",
): Promise<Record<string, unknown>[]> {
  return commands.queryRemoteBrowserDbRows(id, tabIndex, tier);
}

export function restartRemoteBrowserDb(id: string): Promise<void> {
  return commands.restartRemoteBrowserDb(id);
}

export function deleteRemoteBrowserIndexedDbAndWaitForReload(
  id: string,
  dbName: string,
): Promise<void> {
  return commands.deleteRemoteBrowserIndexedDbAndWaitForReload(id, dbName);
}
