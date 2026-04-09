import * as React from "react";
import { JazzProvider, attachDevTools, useJazzClient } from "jazz-tools/react";
import { loadOrCreateIdentitySeed, mintSelfSignedToken, type DbConfig } from "jazz-tools";
import { TodoList } from "./TodoList.js";
import { app } from "../schema.js";

const devToolsAttachedClients = new WeakSet<object>();

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

// #region context-setup-react
function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? readEnvAppId() ?? "019d4349-23f3-7227-818f-51eb1d178b6b";
  const seed = loadOrCreateIdentitySeed(appId);
  const jwtToken = mintSelfSignedToken(seed.seed, appId);

  return {
    appId,
    env: "dev",
    userBranch: "main",
    jwtToken,
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
