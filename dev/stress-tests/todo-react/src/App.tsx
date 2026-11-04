import { useState, useEffect, use, Suspense } from "react";
import { JazzProvider } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { createAccountManager } from "jazz-tools";
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

// Every browser is a local-first guest of the app's backend.
const accountPromise = createAccountManager({ serverUrl }).then(
  (accounts) => accounts.getLoggedIn() ?? accounts.createLocalFirst(),
);

function AppInner() {
  const account = use(accountPromise);
  const config: DbConfig & { asyncSubscriptionsOnly: boolean } = {
    env: import.meta.env.DEV ? "dev" : "prod",
    devMode: import.meta.env.DEV,
    account,
    serverUrl,
    telemetryCollectorUrl,
    logLevel: telemetryCollectorUrl ? "debug" : undefined,
    // The generator batches writes with db.transaction, which only the
    // in-process sync client exposes (the worker-backed async facade has no
    // transaction support).
    asyncSubscriptionsOnly: false,
  };

  return (
    <JazzProvider config={config} fallback={<p>Loading...</p>}>
      <Router />
    </JazzProvider>
  );
}

// #region context-setup-react
export function App() {
  return (
    <Suspense fallback={<p>Loading...</p>}>
      <AppInner />
    </Suspense>
  );
}
// #endregion context-setup-react
