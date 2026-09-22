import { createInspectorAdminClient } from "jazz-tools/_dev/inspector-client";
import { BrowserRouter } from "react-router";
import { JazzClientProvider } from "jazz-tools/react";
import { fetchSchemaHashes, fetchStoredPermissions, fetchStoredWasmSchema } from "jazz-tools";
import { useEffect, useRef, useState } from "react";
import { StandaloneProvider } from "./contexts/standalone-context.js";
import { DevtoolsProvider } from "./contexts/devtools-context.js";
import { InspectorRoutes } from "./routes.js";
import { DbConfigForm, SchemaHashSelect } from "./components/db-config-form/index.js";
import type { DbConfigFormValues } from "./components/db-config-form/index.js";
import { normalizeSchemaHashInfos, type SchemaHashInfo } from "./utility/schema-hash-display.js";
import styles from "./App.module.css";

interface StoredConnection {
  id: string;
  name: string;
  serverUrl: string;
  appId: string;
  env: string;
  schemaHash: string;
}

interface StoredConnections {
  version: 2;
  activeConnectionId: string | null;
  connections: StoredConnection[];
}

type LegacyStoredConfig = Omit<StoredConnection, "id" | "name"> & {
  adminSecret?: string;
  branch?: string;
};

const STORAGE_KEY = "jazz-inspector-standalone-config";
const DEFAULT_SERVER_URL = "https://v2.sync.jazz.tools/";

type AppScreen = "form" | "schema" | "connections" | null;
type ConnectionFormMode = "connect" | "edit";
type SchemaHashesResult = Awaited<ReturnType<typeof fetchSchemaHashes>> & {
  schemas?: SchemaHashInfo[];
};
type InspectorAdminClient = Awaited<ReturnType<typeof createInspectorAdminClient>>;

type SetupState = "pending" | "transferred" | "failed" | "disposed";

interface SetupLifetime {
  resolveClient(client: InspectorAdminClient): void;
  transfer(client: InspectorAdminClient): boolean;
  fail(): void;
  dispose(client?: InspectorAdminClient): void;
}

function createSetupLifetime(
  shutdownClient: (client: InspectorAdminClient) => void,
): SetupLifetime {
  let state: SetupState = "pending";
  let ownedClient: InspectorAdminClient | null = null;
  let shutdownClaimed = false;

  const shutdownOwnedClient = (client: InspectorAdminClient) => {
    if (shutdownClaimed) return;
    shutdownClaimed = true;
    shutdownClient(client);
  };

  return {
    resolveClient(client) {
      if (state === "pending") {
        ownedClient = client;
      } else if (state === "failed" || state === "disposed") {
        shutdownOwnedClient(client);
      }
    },
    transfer(client) {
      if (state !== "pending") {
        if (state === "failed" || state === "disposed") {
          shutdownOwnedClient(client);
        }
        return false;
      }
      state = "transferred";
      ownedClient = null;
      return true;
    },
    fail() {
      if (state !== "pending") return;
      state = "failed";
      if (ownedClient) {
        shutdownOwnedClient(ownedClient);
        ownedClient = null;
      }
    },
    dispose(client) {
      if (state !== "pending") return;
      state = "disposed";
      if (client) ownedClient = client;
      if (ownedClient) {
        shutdownOwnedClient(ownedClient);
        ownedClient = null;
      }
    },
  };
}

export default function App() {
  const [initialState] = useState(() => {
    const connections = readStoredConnections();
    const fragmentConfig = readFragmentConfig();
    const activeConnection = getActiveConnection(connections);

    return {
      connections,
      editingConnectionId: fragmentConfig ? null : (activeConnection?.id ?? null),
      initialFormValues: fragmentConfig ?? storedConnectionToFormValues(activeConnection) ?? null,
      screen: "form" as AppScreen,
    };
  });
  const [connectionStore, setConnectionStore] = useState<StoredConnections>(
    initialState.connections,
  );
  const [screen, setScreen] = useState<AppScreen>(initialState.screen);
  const [connectionFormMode, setConnectionFormMode] = useState<ConnectionFormMode>("connect");
  const [editingConnectionId, setEditingConnectionId] = useState<string | null>(
    initialState.editingConnectionId,
  );
  const [formValues, setFormValues] = useState<DbConfigFormValues | null>(
    initialState.initialFormValues,
  );
  const [activeAdminSecret, setActiveAdminSecret] = useState<string | null>(null);
  const [schemaHashes, setSchemaHashes] = useState<SchemaHashInfo[]>([]);
  const [availableSchemaHashes, setAvailableSchemaHashes] = useState<SchemaHashInfo[]>([]);
  const [client, setClient] = useState<InspectorAdminClient | null>(null);
  const [wasmSchema, setWasmSchema] = useState<import("jazz-tools").WasmSchema | null>(null);
  const [storedPermissions, setStoredPermissions] = useState<Awaited<
    ReturnType<typeof fetchStoredPermissions>
  > | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSwitchingSchema, setIsSwitchingSchema] = useState(false);

  const activeConnection = getActiveConnection(connectionStore);

  const shutdownClaims = useRef(new WeakSet<object>()).current;
  const shutdownClient = (client: InspectorAdminClient) => {
    if (shutdownClaims.has(client)) return;
    shutdownClaims.add(client);
    try {
      Promise.resolve(client.shutdown()).catch(() => undefined);
    } catch {
      // Cleanup failures must not replace the setup error or become unhandled rejections.
    }
  };
  const installedClient = useRef<InspectorAdminClient | null>(null);

  const clearRuntime = () => {
    installedClient.current = null;
    setActiveAdminSecret(null);
    setClient((previousClient) => {
      if (previousClient) {
        shutdownClient(previousClient);
      }
      return null;
    });
    setWasmSchema(null);
    setStoredPermissions(null);
  };

  const updateConnectionStore = (nextStore: StoredConnections) => {
    writeStoredConnections(nextStore);
    setConnectionStore(nextStore);
  };

  const handleFormSubmit = (values: DbConfigFormValues, hashes: SchemaHashInfo[]) => {
    setFormValues(values);
    setSchemaHashes(hashes);
    setScreen("schema");
  };

  const handleSchemaSelect = (schemaHash: string) => {
    if (!formValues || !formValues.adminSecret) return;

    const connectionId = editingConnectionId ?? createConnectionId();
    const connection: StoredConnection = {
      id: connectionId,
      name: formValues.name.trim() || deriveConnectionName(formValues),
      serverUrl: formValues.serverUrl,
      appId: formValues.appId,
      env: formValues.env || "dev",
      schemaHash,
    };
    const existingIndex = connectionStore.connections.findIndex(
      (storedConnection) => storedConnection.id === connectionId,
    );
    const nextConnections =
      existingIndex === -1
        ? [...connectionStore.connections, connection]
        : connectionStore.connections.map((storedConnection) =>
            storedConnection.id === connectionId ? connection : storedConnection,
          );
    const nextStore: StoredConnections = {
      version: 2,
      activeConnectionId: connection.id,
      connections: nextConnections,
    };

    clearRuntime();
    setActiveAdminSecret(formValues.adminSecret);
    updateConnectionStore(nextStore);
    setConnectionFormMode("connect");
    setEditingConnectionId(null);
    setFormValues(null);
    setSchemaHashes([]);
    setScreen(null);
  };

  const handleHeaderSchemaSelect = (schemaHash: string) => {
    if (!activeConnection || activeConnection.schemaHash === schemaHash) return;
    const nextConnection = { ...activeConnection, schemaHash };
    const nextStore = replaceConnection(connectionStore, nextConnection, nextConnection.id);
    const existingAdminSecret = activeAdminSecret;
    setIsSwitchingSchema(true);
    setError(null);
    clearRuntime();
    setActiveAdminSecret(existingAdminSecret);
    updateConnectionStore(nextStore);
  };

  const handleManageConnections = () => {
    clearRuntime();
    setConnectionFormMode("connect");
    setEditingConnectionId(null);
    setFormValues(null);
    setSchemaHashes([]);
    setError(null);
    setIsSwitchingSchema(false);
    setScreen("connections");
  };

  const handleAddConnection = () => {
    setConnectionFormMode("connect");
    setEditingConnectionId(null);
    setFormValues(null);
    setSchemaHashes([]);
    setScreen("form");
  };

  const handleEditConnection = (connectionId: string) => {
    const connection = connectionStore.connections.find(
      (storedConnection) => storedConnection.id === connectionId,
    );
    if (!connection) return;

    setConnectionFormMode("edit");
    setEditingConnectionId(connection.id);
    setFormValues(storedConnectionToFormValues(connection) ?? null);
    setSchemaHashes([]);
    setError(null);
    setIsSwitchingSchema(false);
    if (connection.id === activeConnection?.id) {
      clearRuntime();
    }
    setScreen("form");
  };

  const handleUseConnection = (connectionId: string) => {
    const connection = connectionStore.connections.find(
      (storedConnection) => storedConnection.id === connectionId,
    );
    if (!connection) return;

    const nextStore = { ...connectionStore, activeConnectionId: connectionId };
    clearRuntime();
    setConnectionFormMode("connect");
    setEditingConnectionId(connection.id);
    setFormValues(storedConnectionToFormValues(connection) ?? null);
    setSchemaHashes([]);
    updateConnectionStore(nextStore);
    setScreen("form");
  };

  const handleDeleteConnection = (connectionId: string) => {
    const nextConnections = connectionStore.connections.filter(
      (connection) => connection.id !== connectionId,
    );
    const activeConnectionId =
      connectionStore.activeConnectionId === connectionId
        ? (nextConnections[0]?.id ?? null)
        : connectionStore.activeConnectionId;
    const nextStore: StoredConnections = {
      version: 2,
      activeConnectionId,
      connections: nextConnections,
    };
    if (connectionStore.activeConnectionId === connectionId) {
      clearRuntime();
    }
    updateConnectionStore(nextStore);
    if (nextConnections.length === 0) {
      setScreen("form");
      setFormValues(null);
    }
  };

  useEffect(() => {
    if (!activeConnection || !activeAdminSecret) return;

    let active = true;
    const lifetime = createSetupLifetime(shutdownClient);

    const run = async () => {
      try {
        const clientPromise = createInspectorAdminClient({
          appId: activeConnection.appId,
          serverUrl: activeConnection.serverUrl,
          env: activeConnection.env,
          adminSecret: activeAdminSecret,
        });
        void clientPromise.then(
          (resolvedClient) => lifetime.resolveClient(resolvedClient),
          () => undefined,
        );

        const [resolvedClient, { schema }, schemaHashesResult, permissions] = await Promise.all([
          clientPromise,
          fetchStoredWasmSchema(activeConnection.serverUrl, {
            appId: activeConnection.appId,
            adminSecret: activeAdminSecret,
            schemaHash: activeConnection.schemaHash,
          }),
          fetchSchemaHashes(activeConnection.serverUrl, {
            appId: activeConnection.appId,
            adminSecret: activeAdminSecret,
          }) as Promise<SchemaHashesResult>,
          fetchStoredPermissions(activeConnection.serverUrl, {
            appId: activeConnection.appId,
            adminSecret: activeAdminSecret,
          }).catch(() => null),
        ]);

        if (!active) {
          lifetime.dispose(resolvedClient);
          return;
        }

        if (!lifetime.transfer(resolvedClient)) return;
        installedClient.current = resolvedClient;
        setClient((previousClient) => {
          if (previousClient) {
            shutdownClient(previousClient);
          }
          return resolvedClient;
        });
        setWasmSchema(schema);
        setStoredPermissions(permissions);
        setAvailableSchemaHashes(
          normalizeSchemaHashInfos(schemaHashesResult.hashes, schemaHashesResult.schemas),
        );
        setError(null);
        setIsSwitchingSchema(false);
      } catch (err) {
        lifetime.fail();
        if (!active) return;
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setIsSwitchingSchema(false);
      }
    };
    run();
    return () => {
      active = false;
      lifetime.dispose();
      const installed = installedClient.current;
      installedClient.current = null;
      if (installed) {
        shutdownClient(installed);
      }
    };
  }, [activeConnection, activeAdminSecret]);

  if (screen === "connections") {
    return (
      <main className={styles.statePage}>
        <ConnectionManager
          connections={connectionStore.connections}
          activeConnectionId={connectionStore.activeConnectionId}
          onAddConnection={handleAddConnection}
          onEditConnection={handleEditConnection}
          onUseConnection={handleUseConnection}
          onDeleteConnection={handleDeleteConnection}
        />
      </main>
    );
  }

  if (screen === "form") {
    const initialValues =
      connectionFormMode === "edit"
        ? (formValues ??
          (editingConnectionId
            ? storedConnectionToFormValues(
                connectionStore.connections.find(
                  (connection) => connection.id === editingConnectionId,
                ) ?? activeConnection,
              )
            : undefined))
        : (formValues ?? { serverUrl: DEFAULT_SERVER_URL });
    const formTitle =
      connectionFormMode === "edit"
        ? "Edit connection"
        : connectionStore.connections.length > 0
          ? "Add connection"
          : "Connect to Jazz server";

    return (
      <main className={styles.statePage}>
        <DbConfigForm
          onSubmit={handleFormSubmit}
          initialValues={initialValues}
          mode={connectionFormMode}
          title={formTitle}
          onCancel={connectionStore.connections.length > 0 ? handleManageConnections : undefined}
        />
      </main>
    );
  }

  if (screen === "schema" && formValues) {
    return (
      <main className={styles.statePage}>
        <SchemaHashSelect schemas={schemaHashes} onSelect={handleSchemaSelect} />
      </main>
    );
  }

  if (error) {
    return (
      <main className={styles.statePage}>
        <section className={styles.stateCard}>
          <h2 className={styles.stateTitle}>Connection error</h2>
          <p role="alert" className={styles.errorText}>
            {error}
          </p>
          <div className={styles.actionRow}>
            <button type="button" onClick={handleManageConnections} className={styles.actionButton}>
              Connections
            </button>
          </div>
        </section>
      </main>
    );
  }

  if (!client || !wasmSchema || !activeConnection || !activeAdminSecret) {
    return (
      <main className={styles.statePage}>
        <section className={styles.stateCard}>
          <p className={styles.loadingText}>Loading...</p>
        </section>
      </main>
    );
  }

  return (
    <JazzClientProvider client={client}>
      <DevtoolsProvider
        wasmSchema={wasmSchema}
        storedPermissions={storedPermissions}
        runtime="standalone"
      >
        <StandaloneProvider
          onManageConnections={handleManageConnections}
          schemaHashes={availableSchemaHashes}
          selectedSchemaHash={activeConnection.schemaHash}
          onSelectSchema={handleHeaderSchemaSelect}
          isSwitchingSchema={isSwitchingSchema}
          connection={{
            serverUrl: activeConnection.serverUrl,
            appId: activeConnection.appId,
            adminSecret: activeAdminSecret,
          }}
        >
          <BrowserRouter>
            <InspectorRoutes />
          </BrowserRouter>
        </StandaloneProvider>
      </DevtoolsProvider>
    </JazzClientProvider>
  );
}

interface ConnectionManagerProps {
  connections: StoredConnection[];
  activeConnectionId: string | null;
  onAddConnection: () => void;
  onEditConnection: (connectionId: string) => void;
  onUseConnection: (connectionId: string) => void;
  onDeleteConnection: (connectionId: string) => void;
}

function ConnectionManager({
  connections,
  activeConnectionId,
  onAddConnection,
  onEditConnection,
  onUseConnection,
  onDeleteConnection,
}: ConnectionManagerProps) {
  return (
    <section className={styles.connectionManager}>
      <div className={styles.managerHeader}>
        <div>
          <h2 className={styles.stateTitle}>Connections</h2>
          <p className={styles.managerSubtitle}>Saved standalone Jazz server connections.</p>
        </div>
        <div className={styles.actionRow}>
          <button type="button" onClick={onAddConnection} className={styles.actionButton}>
            Add connection
          </button>
        </div>
      </div>
      {connections.length === 0 ? (
        <p className={styles.emptyText}>No saved connections.</p>
      ) : (
        <div className={styles.connectionList}>
          {connections.map((connection) => {
            const isActive = connection.id === activeConnectionId;

            return (
              <article key={connection.id} className={styles.connectionItem}>
                <div className={styles.connectionDetails}>
                  <div className={styles.connectionTitleRow}>
                    <h3 className={styles.connectionName}>{connection.name}</h3>
                    {isActive ? <span className={styles.activeBadge}>Active</span> : null}
                  </div>
                  <p className={styles.connectionMeta}>{connection.serverUrl}</p>
                  <p className={styles.connectionMeta}>
                    {connection.appId} · {connection.env}
                  </p>
                </div>
                <div className={styles.connectionActions}>
                  <button
                    type="button"
                    onClick={() => onUseConnection(connection.id)}
                    className={styles.actionButton}
                    aria-label={`Open ${connection.name}`}
                  >
                    Open
                  </button>
                  <button
                    type="button"
                    onClick={() => onEditConnection(connection.id)}
                    className={styles.actionButtonSecondary}
                    aria-label={`Edit ${connection.name}`}
                  >
                    Edit
                  </button>
                  <button
                    type="button"
                    onClick={() => onDeleteConnection(connection.id)}
                    className={styles.actionButtonSecondary}
                    aria-label={`Delete ${connection.name}`}
                  >
                    Delete
                  </button>
                </div>
              </article>
            );
          })}
        </div>
      )}
    </section>
  );
}

function storedConnectionToFormValues(
  connection: StoredConnection | null | undefined,
): DbConfigFormValues | undefined {
  if (!connection) return undefined;
  return {
    name: connection.name,
    serverUrl: connection.serverUrl,
    appId: connection.appId,
    adminSecret: "",
    env: connection.env,
  };
}

function readStoredConnections(): StoredConnections {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return emptyConnectionStore();
    const parsed = JSON.parse(raw) as unknown;
    const migrated = migrateStoredConnections(parsed);
    if (!migrated) {
      localStorage.removeItem(STORAGE_KEY);
      return emptyConnectionStore();
    }
    // Migration must remove legacy credentials even when rewriting storage fails.
    localStorage.setItem(STORAGE_KEY, JSON.stringify(migrated));
    return migrated;
  } catch {
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {
      // Storage can be unavailable; do not let cleanup mask the original read failure.
    }
    return emptyConnectionStore();
  }
}

function migrateStoredConnections(parsed: unknown): StoredConnections | null {
  if (isStoredConnections(parsed)) {
    return {
      version: 2,
      activeConnectionId: parsed.activeConnectionId,
      connections: parsed.connections.map(({ id, name, serverUrl, appId, env, schemaHash }) => ({
        id,
        name,
        serverUrl,
        appId,
        env,
        schemaHash,
      })),
    };
  }

  if (isLegacyStoredConfig(parsed)) {
    const legacyConnection: StoredConnection = {
      id: createConnectionId(),
      name: deriveConnectionName(parsed),
      serverUrl: parsed.serverUrl,
      appId: parsed.appId,
      env: parsed.env || "dev",
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
    typeof candidate.schemaHash === "string"
  );
}

function writeStoredConnections(connections: StoredConnections): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(connections));
  } catch {
    // Keep the in-memory connection store responsive when storage is unavailable.
  }
}

function emptyConnectionStore(): StoredConnections {
  return {
    version: 2,
    activeConnectionId: null,
    connections: [],
  };
}

function getActiveConnection(connections: StoredConnections): StoredConnection | null {
  return (
    connections.connections.find(
      (connection) => connection.id === connections.activeConnectionId,
    ) ??
    connections.connections[0] ??
    null
  );
}

function replaceConnection(
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

function createConnectionId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `connection-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function deriveConnectionName(connection: Pick<DbConfigFormValues, "serverUrl" | "appId">): string {
  try {
    const host = new URL(connection.serverUrl).host;
    return host ? `${host} · ${connection.appId}` : connection.appId;
  } catch {
    return connection.appId || "Jazz connection";
  }
}

function readFragmentConfig(): DbConfigFormValues | null {
  const raw = window.location.hash.startsWith("#")
    ? window.location.hash.slice(1)
    : window.location.hash;
  if (!raw) return null;

  const params = new URLSearchParams(raw);
  const hasKnownPrefillParam = ["name", "serverUrl", "appId", "adminSecret", "env"].some((key) =>
    params.has(key),
  );

  if (!hasKnownPrefillParam) {
    return null;
  }

  window.history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);

  return {
    name: (params.get("name") ?? "").trim(),
    serverUrl: (params.get("serverUrl") ?? "").trim(),
    appId: (params.get("appId") ?? "").trim(),
    adminSecret: "",
    env: (params.get("env") ?? "dev").trim() || "dev",
  };
}
