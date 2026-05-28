// Standalone inspector connection state and handlers shared across connection routes.
// The router always mounts; this provider owns runtime lifecycle and exposes CRUD operations.

import { createJazzClient, JazzClientProvider } from "jazz-tools/react";
import { fetchSchemaHashes, fetchStoredPermissions, fetchStoredWasmSchema } from "jazz-tools";
import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren,
} from "react";
import { useLocation, useNavigate, useParams } from "@tanstack/react-router";
import { DevtoolsProvider } from "#contexts/devtools-context";
import { StandaloneProvider } from "#contexts/standalone-context";
import type { DbConfigFormValues } from "#db-config-form/index";
import {
  createConnectionId,
  deriveConnectionName,
  getConnectionById,
  readFragmentConfig,
  readStoredConnections,
  writeStoredConnections,
  type StoredConnection,
  type StoredConnections,
} from "#lib/config/connections";
import { appRoutes } from "#lib/navigation/appRoutes";
import styles from "../App.module.css";

interface StandaloneConnectionContextValue {
  connections: StoredConnection[];
  activeConnectionId: string | null;
  fragmentConfig: DbConfigFormValues | null;
  saveConnectionAndOpen: (
    values: DbConfigFormValues,
    schemaHash: string,
    connectionId?: string,
  ) => Promise<void>;
  deleteConnection: (connectionId: string) => void;
  openConnection: (connectionId: string) => Promise<void>;
  manageConnections: () => void;
}

const StandaloneConnectionContext = createContext<StandaloneConnectionContextValue | null>(null);

export function useStandaloneConnection(): StandaloneConnectionContextValue {
  const context = useContext(StandaloneConnectionContext);
  if (context === null) {
    throw new Error("useStandaloneConnection must be used inside StandaloneConnectionProvider");
  }
  return context;
}

export function StandaloneConnectionProvider({ children }: PropsWithChildren) {
  const navigate = useNavigate();
  const location = useLocation();
  const params = useParams({ strict: false });
  const [fragmentConfig] = useState<DbConfigFormValues | null>(readFragmentConfig);
  const [connectionStore, setConnectionStore] = useState<StoredConnections>(readStoredConnections);
  const [availableSchemaHashes, setAvailableSchemaHashes] = useState<string[]>([]);
  const [client, setClient] = useState<Awaited<ReturnType<typeof createJazzClient>> | null>(null);
  const [wasmSchema, setWasmSchema] = useState<import("jazz-tools").WasmSchema | null>(null);
  const [storedPermissions, setStoredPermissions] = useState<Awaited<
    ReturnType<typeof fetchStoredPermissions>
  > | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSwitchingSchema, setIsSwitchingSchema] = useState(false);

  const routeConnectionId = params.connectionId;
  const routeBranch = params.branch;
  const routeSchemaHash = params.schemaHash;
  const isExtensionRoute = routeConnectionId === "extension";
  const isConnectionManagementRoute =
    location.pathname === "/conn" ||
    location.pathname === "/conn/" ||
    location.pathname === "/conn/new" ||
    location.pathname.startsWith("/conn/edit/");
  const isStandaloneRuntimeRoute =
    isExtensionRoute === false && isConnectionManagementRoute === false;
  const runtimeConnection = useMemo(
    () =>
      resolveRuntimeConnection(connectionStore, {
        branch: routeBranch,
        connectionId: routeConnectionId,
        isStandaloneRuntimeRoute,
        schemaHash: routeSchemaHash,
      }),
    [connectionStore, isStandaloneRuntimeRoute, routeBranch, routeConnectionId, routeSchemaHash],
  );

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

  const saveConnection = (
    values: DbConfigFormValues,
    schemaHash: string,
    connectionId?: string,
  ): StoredConnection => {
    const id = connectionId ?? createConnectionId();
    const trimmedName = values.name.trim();
    const env = values.env.length > 0 ? values.env : "dev";
    const branch = values.branch.length > 0 ? values.branch : "main";
    const connection: StoredConnection = {
      id,
      name: trimmedName.length > 0 ? trimmedName : deriveConnectionName(values),
      serverUrl: values.serverUrl,
      appId: values.appId,
      adminSecret: values.adminSecret,
      env,
      branch,
      schemaHash,
    };
    const existingIndex = connectionStore.connections.findIndex(
      (storedConnection) => storedConnection.id === id,
    );
    const nextConnections =
      existingIndex === -1
        ? [...connectionStore.connections, connection]
        : connectionStore.connections.map((storedConnection) =>
            storedConnection.id === id ? connection : storedConnection,
          );
    const nextStore: StoredConnections = {
      version: 2,
      activeConnectionId: id,
      connections: nextConnections,
    };

    clearRuntime();
    updateConnectionStore(nextStore);
    return connection;
  };

  const openConnection = async (connectionId: string) => {
    const connection = connectionStore.connections.find(
      (storedConnection) => storedConnection.id === connectionId,
    );
    if (connection === undefined) {
      return;
    }

    setActiveConnection(connectionId);
    await navigate({
      to: appRoutes.dataExplorer,
      params: {
        connectionId: connection.id,
        branch: connection.branch,
        schemaHash: connection.schemaHash,
      },
    });
  };

  const saveConnectionAndOpen = async (
    values: DbConfigFormValues,
    schemaHash: string,
    connectionId?: string,
  ) => {
    const connection = saveConnection(values, schemaHash, connectionId);
    await navigate({
      to: appRoutes.dataExplorer,
      params: {
        connectionId: connection.id,
        branch: connection.branch,
        schemaHash,
      },
    });
  };

  const deleteConnection = (connectionId: string) => {
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
  };

  const setActiveConnection = (connectionId: string) => {
    if (connectionStore.activeConnectionId === connectionId) {
      return;
    }

    const nextStore = { ...connectionStore, activeConnectionId: connectionId };
    clearRuntime();
    setError(null);
    setIsSwitchingSchema(false);
    updateConnectionStore(nextStore);
  };

  const selectSchema = (schemaHash: string) => {
    if (runtimeConnection === null || runtimeConnection.schemaHash === schemaHash) return;
    setIsSwitchingSchema(true);
    setError(null);
    clearRuntime();

    // Schema selection is URL state. Saved connections keep defaults for partial route redirects.
    void navigate({
      to: location.pathname.endsWith("/live-query") ? appRoutes.liveQuery : appRoutes.dataExplorer,
      params: {
        connectionId: runtimeConnection.id,
        branch: runtimeConnection.branch,
        schemaHash,
      },
      replace: true,
    });
  };

  const manageConnections = () => {
    setError(null);
    setIsSwitchingSchema(false);
    void navigate({ to: appRoutes.connections });
  };

  useEffect(() => {
    if (runtimeConnection === null || isStandaloneRuntimeRoute === false) return;

    let active = true;

    const run = async () => {
      try {
        const [resolvedClient, { schema }, { hashes }, permissions] = await Promise.all([
          createJazzClient({
            appId: runtimeConnection.appId,
            serverUrl: runtimeConnection.serverUrl,
            env: runtimeConnection.env,
            userBranch: runtimeConnection.branch,
            adminSecret: runtimeConnection.adminSecret,
            driver: { type: "memory" },
          }),
          fetchStoredWasmSchema(runtimeConnection.serverUrl, {
            appId: runtimeConnection.appId,
            adminSecret: runtimeConnection.adminSecret,
            schemaHash: runtimeConnection.schemaHash,
          }),
          fetchSchemaHashes(runtimeConnection.serverUrl, {
            appId: runtimeConnection.appId,
            adminSecret: runtimeConnection.adminSecret,
          }),
          fetchStoredPermissions(runtimeConnection.serverUrl, {
            appId: runtimeConnection.appId,
            adminSecret: runtimeConnection.adminSecret,
          }).catch(() => null),
        ]);

        if (active === false) {
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
        if (active === false) return;
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setIsSwitchingSchema(false);
      }
    };

    run();

    return () => {
      active = false;
    };
  }, [runtimeConnection, isStandaloneRuntimeRoute]);

  const value = useMemo<StandaloneConnectionContextValue>(
    () => ({
      connections: connectionStore.connections,
      activeConnectionId: connectionStore.activeConnectionId,
      fragmentConfig,
      saveConnectionAndOpen,
      deleteConnection,
      openConnection,
      manageConnections,
    }),
    [connectionStore, fragmentConfig],
  );

  if (isExtensionRoute === true || isConnectionManagementRoute === true) {
    return (
      <StandaloneConnectionContext.Provider value={value}>
        {children}
      </StandaloneConnectionContext.Provider>
    );
  }

  if (client !== null && wasmSchema !== null && runtimeConnection !== null) {
    return (
      <JazzClientProvider client={client}>
        <DevtoolsProvider
          wasmSchema={wasmSchema}
          storedPermissions={storedPermissions}
          runtime="standalone"
        >
          <StandaloneProvider
            onManageConnections={manageConnections}
            schemaHashes={availableSchemaHashes}
            selectedSchemaHash={runtimeConnection.schemaHash}
            onSelectSchema={selectSchema}
            isSwitchingSchema={isSwitchingSchema}
            connection={{
              serverUrl: runtimeConnection.serverUrl,
              appId: runtimeConnection.appId,
              adminSecret: runtimeConnection.adminSecret,
            }}
          >
            <StandaloneConnectionContext.Provider value={value}>
              {children}
            </StandaloneConnectionContext.Provider>
          </StandaloneProvider>
        </DevtoolsProvider>
      </JazzClientProvider>
    );
  }

  if (runtimeConnection !== null && error !== null) {
    return (
      <StandaloneConnectionContext.Provider value={value}>
        <ConnectionRuntimeState
          title="Connection error"
          message={error}
          actionLabel="Manage connections"
          onAction={manageConnections}
        />
      </StandaloneConnectionContext.Provider>
    );
  }

  if (runtimeConnection !== null) {
    return (
      <StandaloneConnectionContext.Provider value={value}>
        <ConnectionRuntimeState
          title="Loading connection"
          message="Preparing the inspector runtime."
        />
      </StandaloneConnectionContext.Provider>
    );
  }

  if (isStandaloneRuntimeRoute === true) {
    return (
      <StandaloneConnectionContext.Provider value={value}>
        <ConnectionRuntimeState
          title="Connection not found"
          message="This inspector URL references a connection that is not saved locally."
          actionLabel="Manage connections"
          onAction={manageConnections}
        />
      </StandaloneConnectionContext.Provider>
    );
  }

  return (
    <StandaloneConnectionContext.Provider value={value}>
      {children}
    </StandaloneConnectionContext.Provider>
  );
}

function resolveRuntimeConnection(
  connectionStore: StoredConnections,
  {
    branch,
    connectionId,
    isStandaloneRuntimeRoute,
    schemaHash,
  }: {
    branch: string | undefined;
    connectionId: string | undefined;
    isStandaloneRuntimeRoute: boolean;
    schemaHash: string | undefined;
  },
): StoredConnection | null {
  if (isStandaloneRuntimeRoute === false) {
    return null;
  }

  if (connectionId === undefined || branch === undefined || schemaHash === undefined) {
    return null;
  }

  const storedConnection = getConnectionById(connectionStore, connectionId);
  if (storedConnection === null) {
    return null;
  }

  // Full route params are authoritative; stored values supply credentials and saved defaults only.
  return {
    ...storedConnection,
    branch,
    schemaHash,
  };
}

interface ConnectionRuntimeStateProps {
  title: string;
  message: string;
  actionLabel?: string;
  onAction?: () => void;
}

function ConnectionRuntimeState({
  title,
  message,
  actionLabel,
  onAction,
}: ConnectionRuntimeStateProps): React.ReactElement {
  return (
    <main className={styles.statePage}>
      <section className={styles.stateCard}>
        <h1 className={styles.stateTitle}>{title}</h1>
        <p className={styles.loadingText}>{message}</p>
        {actionLabel !== undefined && onAction !== undefined ? (
          <button type="button" onClick={onAction} className={styles.actionButton}>
            {actionLabel}
          </button>
        ) : null}
      </section>
    </main>
  );
}
