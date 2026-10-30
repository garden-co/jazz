import { commands } from "vitest/browser";

import type { JazzServerInfo } from "./testing-server.js";

export interface JazzServerBrowserCommands {
  jazzServerInfo(appId?: string): Promise<JazzServerInfo>;
  jazzServerStop(serverUrl: string): Promise<void>;
  jazzServerJwtForUser(
    userId: string,
    claims?: Record<string, unknown>,
    appId?: string,
  ): Promise<string>;
}

export interface JazzServerTransportControlBrowserCommands {
  jazzServerTransportControlCreate(serverUrl: string): Promise<string>;
  jazzServerTransportControlBlock(url: string, direction: "both" | "inbound"): Promise<void>;
  jazzServerTransportControlUnblock(url: string): Promise<void>;
  jazzServerTransportControlStop(proxyUrl: string): Promise<void>;
}

export function jazzServerTransportControlBrowserCommands(): JazzServerTransportControlBrowserCommands {
  if (
    !hasFunction(commands, "jazzServerTransportControlCreate") ||
    !hasFunction(commands, "jazzServerTransportControlBlock") ||
    !hasFunction(commands, "jazzServerTransportControlUnblock") ||
    !hasFunction(commands, "jazzServerTransportControlStop")
  ) {
    throw new Error("Browser test project is missing the Jazz server transport-control commands.");
  }
  return commands as unknown as JazzServerTransportControlBrowserCommands;
}

export interface JazzTopologyBrowserCommands {
  jazzBrowserTopologyLog(
    status: "start" | "complete" | "failed",
    label: string,
    elapsedMs: number,
  ): Promise<void>;
}

export interface JazzStorageCorpusBrowserCommands {
  writeBrowserStorageCorpus(records: Record<string, string>): Promise<string | null>;
}

export function jazzStorageCorpusBrowserCommands(): JazzStorageCorpusBrowserCommands {
  if (!hasFunction(commands, "writeBrowserStorageCorpus")) {
    throw new Error("Browser test project is missing the storage corpus export command.");
  }
  return commands as unknown as JazzStorageCorpusBrowserCommands;
}

function hasFunction(value: object, key: string): boolean {
  return key in value && typeof Reflect.get(value, key) === "function";
}

function isJazzServerBrowserCommands(value: unknown): value is JazzServerBrowserCommands {
  return (
    typeof value === "object" &&
    value !== null &&
    hasFunction(value, "jazzServerInfo") &&
    hasFunction(value, "jazzServerStop") &&
    hasFunction(value, "jazzServerJwtForUser")
  );
}

function isJazzTopologyBrowserCommands(value: unknown): value is JazzTopologyBrowserCommands {
  return (
    typeof value === "object" && value !== null && hasFunction(value, "jazzBrowserTopologyLog")
  );
}

/**
 * Read the commands configured by the current browser-test project.
 *
 * Vitest's browser entrypoint re-exports commands through peer dependencies,
 * so test helpers deliberately validate the runtime command contract instead
 * of relying on ambient augmentation from a particular package installation.
 */
export function jazzServerBrowserCommands(): JazzServerBrowserCommands {
  if (!isJazzServerBrowserCommands(commands)) {
    throw new Error(
      "Browser test project is missing Jazz server commands. Configure jazzServerInfo, " +
        "jazzServerStop, and " +
        "jazzServerJwtForUser.",
    );
  }
  return commands;
}

export function jazzTopologyBrowserCommands(): JazzTopologyBrowserCommands {
  if (!isJazzTopologyBrowserCommands(commands)) {
    throw new Error("Browser test project is missing the jazzBrowserTopologyLog command.");
  }
  return commands;
}
