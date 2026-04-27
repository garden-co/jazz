import { BrowserRouter } from "react-router";
import { createJazzClient, JazzClientProvider } from "jazz-tools/react";
import { fetchSchemaHashes, fetchStoredPermissions, fetchStoredWasmSchema } from "jazz-tools";
import { useEffect, useState } from "react";
import { StandaloneProvider } from "./contexts/standalone-context.js";
import { DevtoolsProvider } from "./contexts/devtools-context.js";
import { InspectorRoutes } from "./routes.js";
import { DbConfigForm, SchemaHashSelect } from "./components/db-config-form/index.js";
import type { DbConfigFormValues } from "./components/db-config-form/index.js";
import styles from "./App.module.css";

interface StoredConnection {
  id: string;
  name: string;
  serverUrl: string;
  appId: string;
  adminSecret: string;
  env: string;
  branch: string;
  schemaHash: string;
}

interface StoredConnections {
  version: 2;
  activeConnectionId: string | null;
  connections: StoredConnection[];
}

type LegacyStoredConfig = Omit<StoredConnection, "id" | "name">;

const STORAGE_KEY = "jazz-inspector-standalone-config";
const DEFAULT_SERVER_URL = "https://v2.sync.jazz.tools/";

type AppScreen = "form" | "schema" | "connections" | null;
type ConnectionFormMode = "connect" | "edit";

export default function App() {
  const [initialState] = useState(() => {
    const connections = readStoredConnections();
    const fragmentConfig = readFragmentConfig();
    const activeConnection = getActiveConnection(connections);
    const screen: AppScreen = fragmentConfig || !activeConnection ? "form" : null;

    return {
      connections,
      fragmentConfig,
      screen,
    };
  });
  const [fragmentConfig] = useState<DbConfigFormValues | null>(initialState.fragmentConfig);
  const [connectionStore, setConnectionStore] = useState<StoredConnections>(
    initialState.connections,
  );
  const [screen, setScreen] = useState<AppScreen>(initialState.screen);
  const [connectionFormMode, setConnectionFormMode] = useState<ConnectionFormMode>("connect");
  const [editingConnectionId, setEditingConnectionId] = useState<string | null>(null);
  const [formValues, setFormValues] = useState<DbConfigFormValues | null>(null);
  const [schemaHashes, setSchemaHashes] = useState<string[]>([]);
  const [availableSchemaHashes, setAvailableSchemaHashes] = useState<string[]>([]);
  const [client, setClient] = useState<Awaited<ReturnType<typeof createJazzClient>> | null>(null);
  const [wasmSchema, setWasmSchema] = useState<import("jazz-tools").WasmSchema | null>(null);
  const [storedPermissions, setStoredPermissions] = useState<Awaited<
    ReturnType<typeof fetchStoredPermissions>
  > | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSwitchingSchema, setIsSwitchingSchema] = useState(false);

  const activeConnection = getActiveConnection(connectionStore);

  const clearRuntime = () => {
    setClient((previousClient) => {
      if (previousClient) {
        void previousClient.shutdown();
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

  const handleFormSubmit = (values: DbConfigFormValues, hashes: string[]) => {
    setFormValues(values);
    setSchemaHashes(hashes);
    setScreen("schema");
  };

  const handleSchemaSelect = (schemaHash: string) => {
    if (!formValues) return;

    const connectionId = editingConnectionId ?? createConnectionId();
    const connection: StoredConnection = {
      id: connectionId,
      name: formValues.name.trim() || deriveConnectionName(formValues),
      serverUrl: formValues.serverUrl,
      appId: formValues.appId,
      adminSecret: formValues.adminSecret,
      env: formValues.env || "dev",
      branch: formValues.branch || "main",
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
    setIsSwitchingSchema(true);
    setError(null);
    clearRuntime();
    updateConnectionStore(nextStore);
  };

  const handleManageConnections = () => {
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
    setScreen("form");
  };

  const handleUseConnection = (connectionId: string) => {
    if (connectionStore.activeConnectionId === connectionId) {
      setScreen(null);
      return;
    }
    const nextStore = { ...connectionStore, activeConnectionId: connectionId };
    clearRuntime();
    setError(null);
    setIsSwitchingSchema(false);
    updateConnectionStore(nextStore);
    setScreen(null);
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
    }
  };

  useEffect(() => {
    if (!activeConnection) return;

    let active = true;

    const run = async () => {
      try {
        const [resolvedClient, { schema }, { hashes }, permissions] = await Promise.all([
          createJazzClient({
            appId: activeConnection.appId,
            serverUrl: activeConnection.serverUrl,
            env: activeConnection.env,
            userBranch: activeConnection.branch,
            adminSecret: activeConnection.adminSecret,
            driver: { type: "memory" },
          }),
          fetchStoredWasmSchema(activeConnection.serverUrl, {
            appId: activeConnection.appId,
            adminSecret: activeConnection.adminSecret,
            schemaHash: activeConnection.schemaHash,
          }),
          fetchSchemaHashes(activeConnection.serverUrl, {
            appId: activeConnection.appId,
            adminSecret: activeConnection.adminSecret,
          }),
          fetchStoredPermissions(activeConnection.serverUrl, {
            appId: activeConnection.appId,
            adminSecret: activeConnection.adminSecret,
          }).catch(() => null),
        ]);

        if (!active) {
          void resolvedClient.shutdown();
          return;
        }

        setClient((previousClient) => {
          if (previousClient) {
            void previousClient.shutdown();
          }
          return resolvedClient;
        });
        setWasmSchema(schema);
        setStoredPermissions(permissions);
        setAvailableSchemaHashes(hashes);
        setError(null);
        setIsSwitchingSchema(false);
      } catch (err) {
        if (!active) return;
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setIsSwitchingSchema(false);
      }
    };

    run();

    return () => {
      active = false;
    };
  }, [activeConnection]);

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
        : (formValues ?? fragmentConfig ?? { serverUrl: DEFAULT_SERVER_URL });
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
        <SchemaHashSelect hashes={schemaHashes} onSelect={handleSchemaSelect} />
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

  if (!client || !wasmSchema || !activeConnection) {
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
            adminSecret: activeConnection.adminSecret,
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
                    {connection.appId} · {connection.env}/{connection.branch}
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
    adminSecret: connection.adminSecret,
    env: connection.env,
    branch: connection.branch,
  };
}

function readStoredConnections(): StoredConnections {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return emptyConnectionStore();
    const parsed = JSON.parse(raw) as unknown;
    const migrated = migrateStoredConnections(parsed);
    return migrated ?? emptyConnectionStore();
  } catch {
    return emptyConnectionStore();
  }
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

function writeStoredConnections(connections: StoredConnections): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(connections));
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
