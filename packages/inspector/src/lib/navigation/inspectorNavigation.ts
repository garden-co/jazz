// Resolves partial connection URLs into complete inspector route targets.
// Route loaders use this module so redirect behavior stays out of React components.

import { redirect } from "@tanstack/react-router";
import { fetchSchemaHashes } from "jazz-tools";

import {
  getActiveConnection,
  getConnectionById,
  readStoredConnections,
  type StoredConnection,
  type StoredConnections,
} from "#lib/config/connections.ts";

import { appRoutes } from "./appRoutes.ts";

export interface ResolvedInspectorNavigationTarget {
  connectionId: string;
  branch: string;
  schemaHash: string;
}

interface ResolveInspectorNavigationTargetOptions {
  connectionId: string;
  branchOverride?: string | null;
  schemaHashOverride?: string | null;
  getConnection: (connectionId: string) => StoredConnection | null;
  resolveBranch?: (connection: StoredConnection, branchOverride?: string | null) => string;
  resolveSchemaHash?: (
    connection: StoredConnection,
    availableSchemaHashes: string[],
    schemaHashOverride?: string | null,
  ) => string | null;
}

interface ResolveStoredInspectorNavigationTargetOptions {
  connectionId: string;
  branchOverride?: string | null;
  schemaHashOverride?: string | null;
  store?: StoredConnections;
}

interface ResolveActiveStoredInspectorNavigationTargetOptions {
  store?: StoredConnections;
}

export async function resolveInspectorNavigationTarget({
  connectionId,
  branchOverride,
  schemaHashOverride,
  getConnection,
  resolveBranch = resolveDefaultBranch,
  resolveSchemaHash = resolveDefaultSchemaHash,
}: ResolveInspectorNavigationTargetOptions): Promise<ResolvedInspectorNavigationTarget | null> {
  const connection = getConnection(connectionId);
  if (connection === null) {
    return null;
  }

  const branch = resolveBranch(connection, branchOverride);
  const availableSchemaHashes = await fetchSchemaHashes(connection.serverUrl, {
    appId: connection.appId,
    adminSecret: connection.adminSecret,
  })
    .then((response) => response.hashes)
    .catch(() => []);

  const schemaHash = resolveSchemaHash(connection, availableSchemaHashes, schemaHashOverride);
  if (schemaHash === null) {
    return null;
  }

  return {
    connectionId,
    branch,
    schemaHash,
  };
}

export async function resolveStoredInspectorNavigationTarget({
  connectionId,
  branchOverride,
  schemaHashOverride,
  store,
}: ResolveStoredInspectorNavigationTargetOptions): Promise<ResolvedInspectorNavigationTarget | null> {
  const resolvedStore = store ?? readStoredConnections();

  return resolveInspectorNavigationTarget({
    connectionId,
    branchOverride,
    schemaHashOverride,
    getConnection: (nextConnectionId) => getConnectionById(resolvedStore, nextConnectionId),
  });
}

export async function resolveActiveStoredInspectorNavigationTarget({
  store,
}: ResolveActiveStoredInspectorNavigationTargetOptions = {}): Promise<ResolvedInspectorNavigationTarget | null> {
  const resolvedStore = store ?? readStoredConnections();
  const activeConnection = getActiveConnection(resolvedStore);
  if (activeConnection === null) {
    return null;
  }

  return resolveStoredInspectorNavigationTarget({
    connectionId: activeConnection.id,
    store: resolvedStore,
  });
}

export function redirectToConnections(): never {
  throw redirect({ to: appRoutes.connections });
}

export function redirectToDataExplorerTarget(target: ResolvedInspectorNavigationTarget): never {
  throw redirect({
    to: appRoutes.dataExplorer,
    params: target,
  });
}

function resolveDefaultBranch(
  connection: StoredConnection,
  branchOverride?: string | null,
): string {
  const normalizedOverride = branchOverride?.trim();
  if (normalizedOverride !== undefined && normalizedOverride.length > 0) {
    return normalizedOverride;
  }

  return connection.branch.trim() || "main";
}

function resolveDefaultSchemaHash(
  connection: StoredConnection,
  availableSchemaHashes: string[],
  schemaHashOverride?: string | null,
): string | null {
  const normalizedOverride = schemaHashOverride?.trim();
  if (normalizedOverride !== undefined && normalizedOverride.length > 0) {
    return normalizedOverride;
  }

  if (availableSchemaHashes.includes(connection.schemaHash)) {
    return connection.schemaHash;
  }

  return availableSchemaHashes[0] ?? connection.schemaHash ?? null;
}
