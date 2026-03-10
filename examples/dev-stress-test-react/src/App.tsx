import * as React from "react";
import {
  createJazzClient,
  JazzProvider,
  getActiveSyntheticAuth,
  attachDevTools,
} from "jazz-tools/react";
import { DataPage } from "./DataPage.js";
import { FixtureGeneratorPage } from "./FixtureGeneratorPage.js";
import { app } from "../schema/app.js";

type JazzProviderClientConfig = NonNullable<Parameters<typeof createJazzClient>[0]>;
type AppRoute = "todos" | "generate";

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

// #region context-setup-react
function defaultConfig(
  overrides: Partial<JazzProviderClientConfig> = {},
): JazzProviderClientConfig {
  const appId = overrides.appId ?? readEnvAppId() ?? "6316f08d-d5d1-41df-82b8-8c16aa26db84";
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });

  return {
    appId,
    env: "dev",
    userBranch: "main",
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
    ...overrides,
  };
}
// #endregion context-setup-react

type AppProps = {
  config?: Partial<JazzProviderClientConfig>;
  fallback?: React.ReactNode;
};

// #region context-setup-react
export function App({ config, fallback }: AppProps = {}) {
  const resolvedConfig = defaultConfig(config);
  const configKey = JSON.stringify(resolvedConfig);
  const [client, setClient] = React.useState<Awaited<ReturnType<typeof createJazzClient>> | null>(
    null,
  );
  const [error, setError] = React.useState<unknown>(null);
  const [route, setRoute] = React.useState<AppRoute>(() => {
    return window.location.hash === "#/generate" ? "generate" : "todos";
  });

  const navigate = React.useCallback((next: AppRoute) => {
    const nextHash = next === "generate" ? "#/generate" : "#/todos";
    if (window.location.hash !== nextHash) {
      window.location.hash = nextHash;
    } else {
      setRoute(next);
    }
  }, []);

  React.useEffect(() => {
    const handleHashChange = () => {
      setRoute(window.location.hash === "#/generate" ? "generate" : "todos");
    };
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  React.useEffect(() => {
    let active = true;
    const pending = createJazzClient(resolvedConfig);

    void pending.then(
      (resolved) => {
        if (!active) {
          void resolved.shutdown();
          return;
        }
        setClient(resolved);
        attachDevTools(resolved, app.wasmSchema);
        if (location.origin.includes("localhost")) {
          Object.defineProperty(window, "jazzClient", {
            value: resolved,
            writable: true,
          });
        }
      },
      (reason) => {
        if (!active) return;
        setError(reason);
      },
    );

    return () => {
      active = false;
      void pending.then((resolved) => resolved.shutdown()).catch(() => {});
    };
  }, [configKey]);

  if (error) {
    throw error;
  }

  if (!client) {
    return <>{fallback ?? <p>Loading...</p>}</>;
  }

  return (
    <JazzProvider client={client}>
      <nav>
        <button type="button" onClick={() => navigate("todos")} disabled={route === "todos"}>
          Todos
        </button>
        <button type="button" onClick={() => navigate("generate")} disabled={route === "generate"}>
          Generate Fixtures
        </button>
      </nav>
      {route === "todos" ? (
        <>
          <h1>Todos</h1>
          <DataPage />
        </>
      ) : (
        <FixtureGeneratorPage onNavigateTodos={() => navigate("todos")} />
      )}
    </JazzProvider>
  );
}
// #endregion context-setup-react
