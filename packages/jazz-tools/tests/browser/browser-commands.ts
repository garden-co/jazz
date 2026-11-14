import { commands } from "vitest/browser";

import {
  TRANSPORT_CONTROL_COMMAND_NAMES,
  type JazzServerTransportControlBrowserCommands,
} from "./transport-control-commands.js";

import type { JazzServerInfo } from "./testing-server.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import type { CompiledPermissionsMap } from "../../src/schema-permissions.js";
import type { AppArtifact, AppPermissions } from "../../src/runtime/app-artifact.js";
import type { BackendPermissions, DeployResult } from "../../src/dev/catalogue.js";

export interface JazzServerBrowserCommands {
  jazzServerInfo(appId?: string): Promise<JazzServerInfo>;
  jazzServerStop(serverUrl: string): Promise<void>;
  jazzAppFront(
    coreUrl: string,
    app: {
      schema: WasmSchema;
      /** What the app ships (`defineApp(schema, { artifact })`): a command drops the schema's own record of it. */
      artifact?: AppArtifact;
      permissions: BackendPermissions;
      pageOrigin: string;
    },
  ): Promise<string>;
  jazzAppUser(frontUrl: string, userId: string, claims?: Record<string, unknown>): Promise<string>;
  /** Optional: only configs whose tests sign users out register it. */
  jazzAppSignOut?(frontUrl: string, userUrl: string): Promise<void>;
  /** Optional: only configs whose tests sign guests up register these. */
  jazzAppVisitor?(frontUrl: string): Promise<string>;
  jazzAppSignUp?(frontUrl: string, originUrl: string, userId: string): Promise<void>;
  /** Optional: only configs whose tests write as the backend register it. */
  jazzAppBackendInsert?(
    frontUrl: string,
    table: string,
    row: Record<string, unknown>,
  ): Promise<Record<string, unknown>>;
  jazzAppRequest(
    url: string,
    init: { method: string; headers: [string, string][]; body: string | null },
  ): Promise<{ status: number; headers: [string, string][]; body: string }>;
}

export type { JazzServerTransportControlBrowserCommands };

export function jazzServerTransportControlBrowserCommands(): JazzServerTransportControlBrowserCommands {
  if (!TRANSPORT_CONTROL_COMMAND_NAMES.every((name) => hasFunction(commands, name))) {
    throw new Error("Browser test project is missing the Jazz server transport-control commands.");
  }
  return commands as unknown as JazzServerTransportControlBrowserCommands;
}

export interface JazzDeployBrowserCommands {
  /** See `jazzDeploy` in testing-server-node.ts. */
  jazzDeploy(options: {
    appId: string;
    serverUrl: string;
    adminSecret: string;
    schema: WasmSchema;
    permissions: CompiledPermissionsMap;
  }): Promise<DeployResult>;
}

export function jazzDeployBrowserCommands(): JazzDeployBrowserCommands {
  if (!hasFunction(commands, "jazzDeploy")) {
    throw new Error("Browser test project is missing the jazzDeploy command.");
  }
  return commands as unknown as JazzDeployBrowserCommands;
}

export interface JazzDeployAppBrowserCommands {
  /** See `jazzDeployApp` in testing-server-node.ts. */
  jazzDeployApp(options: {
    appId: string;
    serverUrl: string;
    adminSecret: string;
    schema: WasmSchema;
    artifact?: AppArtifact;
    permissions: BackendPermissions;
  }): Promise<DeployResult>;
}

export function jazzDeployAppBrowserCommands(): JazzDeployAppBrowserCommands {
  if (!hasFunction(commands, "jazzDeployApp")) {
    throw new Error("Browser test project is missing the jazzDeployApp command.");
  }
  return commands as unknown as JazzDeployAppBrowserCommands;
}

export interface JazzAppArtifactBrowserCommands {
  /** See `jazzAppArtifact` in testing-server-node.ts: a browser can't build one. */
  jazzAppArtifact(
    versions: WasmSchema[],
    migrations: unknown[][],
    permissions?: CompiledPermissionsMap,
  ): Promise<{ artifact: AppArtifact; permissions: AppPermissions }>;
}

export function jazzAppArtifactBrowserCommands(): JazzAppArtifactBrowserCommands {
  if (!hasFunction(commands, "jazzAppArtifact")) {
    throw new Error("Browser test project is missing the jazzAppArtifact command.");
  }
  return commands as unknown as JazzAppArtifactBrowserCommands;
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

export interface StorageQuotaBrowserCommands {
  /** Cap `origin`'s storage quota at `quotaSize` bytes, or restore it (`null`). */
  overrideStorageQuota(origin: string, quotaSize: number | null): Promise<void>;
}

export function storageQuotaBrowserCommands(): StorageQuotaBrowserCommands {
  if (!hasFunction(commands, "overrideStorageQuota")) {
    throw new Error("Browser test project is missing the storage quota command.");
  }
  return commands as unknown as StorageQuotaBrowserCommands;
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
    hasFunction(value, "jazzAppFront") &&
    hasFunction(value, "jazzAppUser") &&
    hasFunction(value, "jazzAppRequest")
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
        "jazzServerStop, jazzAppFront, jazzAppUser and jazzAppRequest.",
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
