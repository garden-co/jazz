import * as React from "react";
import { createJazzClient, JazzProvider, getActiveSyntheticAuth } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

type JazzProviderClientConfig = NonNullable<Parameters<typeof createJazzClient>[0]>;

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

// #region context-setup-react
function defaultConfig(
  overrides: Partial<JazzProviderClientConfig> = {},
): JazzProviderClientConfig {
  const appId = overrides.appId ?? readEnvAppId() ?? "todo-react-example";
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

export function App({ config, fallback }: AppProps = {}) {
  const resolvedConfig = defaultConfig(config);
  const configKey = JSON.stringify(resolvedConfig);
  const [client, setClient] = React.useState<Awaited<ReturnType<typeof createJazzClient>> | null>(
    null,
  );
  const [error, setError] = React.useState<unknown>(null);

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
    <>
      <JazzProvider client={client}>
        <h1>Todos</h1>
        <TodoList />
      </JazzProvider>
    </>
  );
}
