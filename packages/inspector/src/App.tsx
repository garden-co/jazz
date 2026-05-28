import { createJazzClient, JazzClientProvider } from "jazz-tools/react";
import { fetchSchemaHashes, fetchStoredPermissions, fetchStoredWasmSchema } from "jazz-tools";
import { useEffect, useState } from "react";
import { StandaloneProvider } from "./contexts/standalone-context.js";
import { DevtoolsProvider } from "./contexts/devtools-context.js";
import { InspectorRouterProvider, createInspectorRouter } from "./createInspectorRouter.js";
import { DbConfigForm, SchemaHashSelect } from "./components/db-config-form/index.js";
import type { DbConfigFormValues } from "./components/db-config-form/index.js";
import {
  createConnectionId,
  deriveConnectionName,
  getActiveConnection,
  readFragmentConfig,
  readStoredConnections,
  replaceConnection,
  writeStoredConnections,
  type StoredConnection,
  type StoredConnections,
} from "#lib/config/connections.ts";
import { appRoutes } from "#lib/navigation/appRoutes.ts";
import styles from "./App.module.css";

const DEFAULT_SERVER_URL = "https://v2.sync.jazz.tools/";

type AppScreen = "form" | "schema" | "connections" | null;
type ConnectionFormMode = "connect" | "edit";

export default function App() {
  const [router] = useState(() => createInspectorRouter());
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

    void router.navigate({
      to: appRoutes.dataExplorer,
      params: {
        connectionId: nextConnection.id,
        branch: nextConnection.branch,
        schemaHash,
      },
      replace: true,
    });
  };

  const handleManageConnections = () => {
    setConnectionFormMode("connect");
    setEditingConnectionId(null);
    setFormValues(null);
    setSchemaHashes([]);
    setError(null);
    setIsSwitchingSchema(false);
    setScreen("connections");
    void router.navigate({ to: appRoutes.connections });
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
          <InspectorRouterProvider router={router} />
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
