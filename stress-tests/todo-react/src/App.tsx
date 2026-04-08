import { useState, useEffect } from "react";
import {
  JazzProvider,
  getActiveSyntheticAuth,
  attachDevTools,
  useJazzClient,
} from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { TodoList } from "./TodoList.js";
import { GenerateData } from "./GenerateData.js";
import { app } from "../schema";

const devToolsAttachedClients = new WeakSet<object>();

function DevToolsRegistration() {
  const client = useJazzClient();

  useEffect(() => {
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

function useHash() {
  const [hash, setHash] = useState(location.hash);
  useEffect(() => {
    const onHashChange = () => setHash(location.hash);
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);
  return hash;
}

function Router() {
  const hash = useHash();

  if (hash === "#list") {
    return (
      <>
        <h1>Todos</h1>
        <p>
          <a href="#">Back to Generate</a>
        </p>
        <TodoList />
      </>
    );
  }

  return <GenerateData />;
}

// #region context-setup-react
export function App() {
  const appId = import.meta.env.JAZZ_APP_ID;
  const serverUrl = import.meta.env.JAZZ_SERVER_URL;

  if (!appId) {
    throw new Error("JAZZ_APP_ID is required");
  }

  if (!serverUrl) {
    throw new Error("JAZZ_SERVER_URL is required");
  }

  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });
  const config: DbConfig = {
    appId,
    env: import.meta.env.DEV ? "dev" : "prod",
    userBranch: "main",
    devMode: import.meta.env.DEV,
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
    serverUrl,
  };

  return (
    <JazzProvider config={config} fallback={<p>Loading...</p>}>
      <DevToolsRegistration />
      <Router />
    </JazzProvider>
  );
}
// #endregion context-setup-react
