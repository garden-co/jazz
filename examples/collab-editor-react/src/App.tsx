import * as React from "react";
import { JazzProvider, attachDevTools, useJazzClient, useLocalFirstAuth } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import Router from "./components/Router.js";
import { Dashboard } from "./components/Dashboard.js";
import { Editor } from "./components/Editor.js";
import { app } from "../schema.js";

const devToolsAttachedClients = new WeakSet<object>();

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
// Optional: VITE_JAZZ_DRIVER=memory keeps everything in memory and syncs purely
// through the server (requires VITE_JAZZ_SERVER_URL). Handy for multi-window
// collaboration demos where local OPFS persistence isn't needed.
const driver: DbConfig["driver"] | undefined =
  import.meta.env.VITE_JAZZ_DRIVER === "memory" ? { type: "memory" } : undefined;

// #region context-setup-react
function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl,
    secret,
    ...(driver ? { driver } : {}),
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

function EditorRoute({ params }: { params?: Record<string, string> }) {
  return <Editor shareToken={params?.shareToken ?? ""} />;
}

// #region context-setup-react
export function App({ config, fallback }: AppProps = {}) {
  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) {
    return <>{fallback ?? <p>Loading...</p>}</>;
  }

  const resolvedConfig = defaultConfig(secret, config);

  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <DevToolsRegistration />
      <Router
        routes={[
          { path: "/", component: Dashboard },
          { path: "/r/:shareToken", component: EditorRoute },
        ]}
      />
    </JazzProvider>
  );
}
// #endregion context-setup-react
