import * as React from "react";
import { JazzProvider, attachDevTools, useJazzClient, useLocalFirstAuth } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { TodoList } from "./TodoList.js";
import { app } from "../schema.js";

const devToolsAttachedClients = new WeakSet<object>();

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
const subscriptionMode = import.meta.env.VITE_JAZZ_SUBSCRIPTION_MODE ?? "async";

// #region context-setup-react
function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl,
    secret,
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

    if ("setDevMode" in client.db) {
      void attachDevTools(client, app.wasmSchema);
    }
    devToolsAttachedClients.add(client as object);

    if (["localhost", "127.0.0.1"].includes(location.hostname)) {
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
  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) {
    return <>{fallback ?? <p>Loading...</p>}</>;
  }

  const resolvedConfig = defaultConfig(secret, config);
  const asyncSubscriptionsOnly = subscriptionMode !== "sync";

  return (
    <JazzProvider
      config={{ ...resolvedConfig, asyncSubscriptionsOnly }}
      fallback={fallback ?? <p>Loading...</p>}
    >
      <DevToolsRegistration />
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react
