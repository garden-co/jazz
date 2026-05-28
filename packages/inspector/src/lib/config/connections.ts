// Standalone inspector connection persistence and migration helpers.
// Credentials stay in local storage and are referenced from shareable routes by connection id.

export interface StoredConnection {
  id: string;
  name: string;
  serverUrl: string;
  appId: string;
  adminSecret: string;
  env: string;
  branch: string;
  schemaHash: string;
}

export interface StoredConnections {
  version: 2;
  activeConnectionId: string | null;
  connections: StoredConnection[];
}

export interface ConnectionFormValues {
  name: string;
  serverUrl: string;
  appId: string;
  adminSecret: string;
  env: string;
  branch: string;
}

type LegacyStoredConfig = Omit<StoredConnection, "id" | "name">;

export const STORAGE_KEY = "jazz-inspector-standalone-config";

export function readStoredConnections(): StoredConnections {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return emptyConnectionStore();
    const parsed = JSON.parse(raw) as unknown;
    const migrated = migrateStoredConnections(parsed);
    if (migrated === null) {
      return emptyConnectionStore();
    }
    if (isStoredConnections(parsed) === false) {
      writeStoredConnections(migrated);
    }
    return migrated;
  } catch {
    return emptyConnectionStore();
  }
}

export function writeStoredConnections(connections: StoredConnections): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(connections));
}

export function emptyConnectionStore(): StoredConnections {
  return {
    version: 2,
    activeConnectionId: null,
    connections: [],
  };
}

export function getConnectionById(
  connections: StoredConnections,
  connectionId: string,
): StoredConnection | null {
  return connections.connections.find((connection) => connection.id === connectionId) ?? null;
}

export function getActiveConnection(connections: StoredConnections): StoredConnection | null {
  return (
    connections.connections.find(
      (connection) => connection.id === connections.activeConnectionId,
    ) ??
    connections.connections[0] ??
    null
  );
}

export function replaceConnection(
  connections: StoredConnections,
  nextConnection: StoredConnection,
  activeConnectionId: string,
): StoredConnections {
  return {
    version: 2,
    activeConnectionId,
    connections: connections.connections.map((connection) =>
      connection.id === nextConnection.id ? nextConnection : connection,
    ),
  };
}

export function createConnectionId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `connection-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

export function deriveConnectionName(
  connection: Pick<ConnectionFormValues, "serverUrl" | "appId">,
): string {
  try {
    const host = new URL(connection.serverUrl).host;
    return host ? `${host} · ${connection.appId}` : connection.appId;
  } catch {
    return connection.appId || "Jazz connection";
  }
}

export function readFragmentConfig(): ConnectionFormValues | null {
  const raw = window.location.hash.startsWith("#")
    ? window.location.hash.slice(1)
    : window.location.hash;
  if (!raw) return null;

  const params = new URLSearchParams(raw);
  const hasKnownPrefillParam = ["name", "serverUrl", "appId", "adminSecret", "env", "branch"].some(
    (key) => params.has(key),
  );

  if (!hasKnownPrefillParam) {
    return null;
  }

  return {
    name: (params.get("name") ?? "").trim(),
    serverUrl: (params.get("serverUrl") ?? "").trim(),
    appId: (params.get("appId") ?? "").trim(),
    adminSecret: (params.get("adminSecret") ?? "").trim(),
    env: (params.get("env") ?? "dev").trim() || "dev",
    branch: (params.get("branch") ?? "main").trim() || "main",
  };
}

function migrateStoredConnections(parsed: unknown): StoredConnections | null {
  if (isStoredConnections(parsed)) {
    return {
      version: 2,
      activeConnectionId: parsed.activeConnectionId,
      connections: parsed.connections,
    };
  }

  if (isLegacyStoredConfig(parsed)) {
    const legacyConnection: StoredConnection = {
      id: createConnectionId(),
      name: deriveConnectionName(parsed),
      serverUrl: parsed.serverUrl,
      appId: parsed.appId,
      adminSecret: parsed.adminSecret,
      env: parsed.env || "dev",
      branch: parsed.branch || "main",
      schemaHash: parsed.schemaHash,
    };

    return {
      version: 2,
      activeConnectionId: legacyConnection.id,
      connections: [legacyConnection],
    };
  }

  return null;
}

function isStoredConnections(value: unknown): value is StoredConnections {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as StoredConnections;
  return (
    candidate.version === 2 &&
    (candidate.activeConnectionId === null || typeof candidate.activeConnectionId === "string") &&
    Array.isArray(candidate.connections) &&
    candidate.connections.every(isStoredConnection)
  );
}

function isStoredConnection(value: unknown): value is StoredConnection {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as StoredConnection;
  return (
    typeof candidate.id === "string" &&
    typeof candidate.name === "string" &&
    isLegacyStoredConfig(candidate)
  );
}

function isLegacyStoredConfig(value: unknown): value is LegacyStoredConfig {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as LegacyStoredConfig;
  return (
    typeof candidate.serverUrl === "string" &&
    typeof candidate.appId === "string" &&
    typeof candidate.adminSecret === "string" &&
    typeof candidate.schemaHash === "string"
  );
}
