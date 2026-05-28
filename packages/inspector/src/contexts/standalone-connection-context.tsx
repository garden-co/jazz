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
  getActiveConnection,
  readFragmentConfig,
  readStoredConnections,
  replaceConnection,
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

  const activeConnection = getActiveConnection(connectionStore);
  const isExtensionRoute = params.connectionId === "extension";
  const isConnectionManagementRoute =
    location.pathname === "/conn" ||
    location.pathname === "/conn/" ||
    location.pathname === "/conn/new" ||
    location.pathname.startsWith("/conn/edit/");
  const isStandaloneRuntimeRoute =
    isExtensionRoute === false && isConnectionManagementRoute === false;

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
    if (activeConnection === null || activeConnection.schemaHash === schemaHash) return;
    const nextConnection = { ...activeConnection, schemaHash };
    const nextStore = replaceConnection(connectionStore, nextConnection, nextConnection.id);
    setIsSwitchingSchema(true);
    setError(null);
    clearRuntime();
    updateConnectionStore(nextStore);

    void navigate({
      to: appRoutes.dataExplorer,
      params: {
        connectionId: nextConnection.id,
        branch: nextConnection.branch,
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
    if (activeConnection === null || isStandaloneRuntimeRoute === false) return;

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
  }, [activeConnection, isStandaloneRuntimeRoute]);

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

  if (client !== null && wasmSchema !== null && activeConnection !== null) {
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
            selectedSchemaHash={activeConnection.schemaHash}
            onSelectSchema={selectSchema}
            isSwitchingSchema={isSwitchingSchema}
            connection={{
              serverUrl: activeConnection.serverUrl,
              appId: activeConnection.appId,
              adminSecret: activeConnection.adminSecret,
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

  if (activeConnection !== null && error !== null) {
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

  if (activeConnection !== null) {
    return (
      <StandaloneConnectionContext.Provider value={value}>
        <ConnectionRuntimeState
          title="Loading connection"
          message="Preparing the inspector runtime."
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
