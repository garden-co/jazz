import * as React from "react";
import { JazzProvider, attachDevTools, useJazzClient } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { BrowserAuthSecretStore } from "jazz-tools";
import { TodoList } from "./TodoList.js";
import { app } from "../schema.js";

const devToolsAttachedClients = new WeakSet<object>();

const appId = import.meta.env.JAZZ_APP_ID;
const serverUrl = import.meta.env.JAZZ_SERVER_URL;

// #region context-setup-react
function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl,
    auth: { localFirstSecret: secret },
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
  return (
    <React.Suspense fallback={fallback ?? <p>Loading...</p>}>
      <AppInner config={config} fallback={fallback} />
    </React.Suspense>
  );
}

function AppInner({ config, fallback }: AppProps) {
  const secret = React.use(BrowserAuthSecretStore.getOrCreateSecret());
  const resolvedConfig = defaultConfig(secret, config);

  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <DevToolsRegistration />
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react
