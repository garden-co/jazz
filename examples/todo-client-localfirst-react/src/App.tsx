import * as React from "react";
import {
  JazzProvider,
  getActiveSyntheticAuth,
  attachDevTools,
  useJazzClient,
} from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { TodoList } from "./TodoList.js";
import { app } from "../schema.js";

const devToolsAttachedClients = new WeakSet<object>();

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

// #region context-setup-react
function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? readEnvAppId() ?? "6316f08d-d5d1-41df-82b8-8c16aa26db84";
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });

  return {
    appId,
    env: "dev",
    userBranch: "main",
    devMode: true,
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
    ...overrides,
  };
}
// #endregion context-setup-react

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
};

function DevToolsRegistration() {
  const client = useJazzClient();

  React.useEffect(() => {
    if (devToolsAttachedClients.has(client as object)) {
      return;
    }

    void attachDevTools(client, app.wasmSchema);
    devToolsAttachedClients.add(client as object);

    if (location.origin.includes("localhost")) {
      Object.defineProperty(window, "jazzClient", {
        value: client,
        writable: true,
      });
    }
  }, [client]);

  return null;
}

// #region context-setup-react
export function App({ config, fallback }: AppProps = {}) {
  const resolvedConfig = defaultConfig(config);

  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <DevToolsRegistration />
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react
