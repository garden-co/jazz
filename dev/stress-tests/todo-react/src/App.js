import { jsx as _jsx, Fragment as _Fragment, jsxs as _jsxs } from "react/jsx-runtime";
import { useState, useEffect, use, Suspense } from "react";
import { JazzProvider } from "jazz-tools/react";
import { BrowserAuthSecretStore } from "jazz-tools";
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
    return _jsxs(_Fragment, {
      children: [
        _jsx("h1", { children: "Todos" }),
        _jsx("p", { children: _jsx("a", { href: "#", children: "Back to Generate" }) }),
        _jsx(TodoList, {}),
      ],
    });
  }
  return _jsx(GenerateData, {});
}
const appId = requireEnv(import.meta.env.VITE_JAZZ_APP_ID, "JAZZ_APP_ID");
const serverUrl = requireEnv(import.meta.env.VITE_JAZZ_SERVER_URL, "JAZZ_SERVER_URL");
const telemetryCollectorUrl = import.meta.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;
function requireEnv(value, name) {
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}
function AppInner() {
  const secret = use(BrowserAuthSecretStore.getOrCreateSecret());
  const config = {
    appId,
    env: import.meta.env.DEV ? "dev" : "prod",
    userBranch: "main",
    devMode: import.meta.env.DEV,
    secret,
    serverUrl,
    telemetryCollectorUrl,
    logLevel: telemetryCollectorUrl ? "debug" : undefined,
    // The generator batches writes with db.transaction, which only the
    // in-process sync client exposes (the worker-backed async facade has no
    // transaction support).
    asyncSubscriptionsOnly: false,
  };
  return _jsx(JazzProvider, {
    config: config,
    fallback: _jsx("p", { children: "Loading..." }),
    children: _jsx(Router, {}),
  });
}
// #region context-setup-react
export function App() {
  return _jsx(Suspense, {
    fallback: _jsx("p", { children: "Loading..." }),
    children: _jsx(AppInner, {}),
  });
}
