import { useState, useEffect } from "react";
import { JazzProvider, type JazzClientConfig } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";
import { GenerateData } from "./GenerateData.js";

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

// The app's backend serves Jazz on this origin (see jazz.server.ts).
const serverUrl = `${location.origin}/jazz`;
const telemetryCollectorUrl = import.meta.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;

// Every browser is a device account (a guest) of the app's backend. The
// generator batches writes with db.transaction, which only the local-first
// client has (in-process, not the worker-backed async facade), so it opens
// at once rather than starting thin.
const config: JazzClientConfig & { asyncSubscriptionsOnly: boolean } = {
  env: import.meta.env.DEV ? "dev" : "prod",
  devMode: import.meta.env.DEV,
  serverUrl,
  initial: "device",
  client: () => import("jazz-tools/local-first"),
  localFirst: "now",
  telemetryCollectorUrl,
  logLevel: telemetryCollectorUrl ? "debug" : undefined,
  asyncSubscriptionsOnly: false,
};

// #region context-setup-react
export function App() {
  return (
    <JazzProvider config={config} loading={<p>Loading...</p>}>
      <Router />
    </JazzProvider>
  );
}
// #endregion context-setup-react
