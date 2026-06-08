import { useState, useEffect, use, Suspense } from "react";
import { JazzProvider, attachDevTools, useJazzClient } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { BrowserAuthSecretStore } from "jazz-tools";
import { TodoList } from "./TodoList.js";
import { GenerateData } from "./GenerateData.js";
import { BenchmarkRunner } from "./BenchmarkRunner.js";
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
  const params = new URLSearchParams(location.search);
  const benchmarkPhase = params.get("benchmark");

  if (benchmarkPhase === "write" || benchmarkPhase === "reopen") {
    return <BenchmarkRunner phase={benchmarkPhase} />;
  }

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

const appId = requireEnv(import.meta.env.VITE_JAZZ_APP_ID, "JAZZ_APP_ID");
const serverUrlEnv = import.meta.env.VITE_JAZZ_SERVER_URL;
const telemetryCollectorUrl = import.meta.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;

function requireEnv(value: string | undefined, name: string): string {
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function optionalPositiveIntParam(params: URLSearchParams, name: string): number | undefined {
  const raw = params.get(name);
  if (!raw) return undefined;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function AppInner() {
  const secret = use(BrowserAuthSecretStore.getOrCreateSecret());
  const params = new URLSearchParams(location.search);
  const dbName = params.get("dbName") ?? undefined;
  const isBenchmark = params.has("benchmark");
  const benchmarkSyncDisabled = isBenchmark && params.get("sync") === "off";
  const serverUrl = benchmarkSyncDisabled ? undefined : requireEnv(serverUrlEnv, "JAZZ_SERVER_URL");
  const workerInitTimeoutMs =
    optionalPositiveIntParam(params, "workerInitTimeoutMs") ?? (isBenchmark ? 120_000 : undefined);
  const config: DbConfig = {
    appId,
    env: import.meta.env.DEV ? "dev" : "prod",
    userBranch: "main",
    devMode: import.meta.env.DEV,
    secret,
    ...(serverUrl ? { serverUrl } : {}),
    telemetryCollectorUrl,
    logLevel: telemetryCollectorUrl ? "debug" : undefined,
    ...(dbName ? { driver: { type: "persistent" as const, dbName } } : {}),
    ...(workerInitTimeoutMs ? { workerInitTimeoutMs } : {}),
  };

  return (
    <JazzProvider config={config} fallback={<p>Loading...</p>}>
      <DevToolsRegistration />
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
