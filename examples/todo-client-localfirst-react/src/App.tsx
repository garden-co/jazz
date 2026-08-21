import * as React from "react";
import { JazzProvider, useLocalFirstAuth } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { TodoList } from "./TodoList.js";

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

// #region context-setup-react
function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId,
    env: "dev",
    serverUrl,
    secret,
    ...overrides,
  };
}
// #endregion context-setup-react

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
  children?: React.ReactNode;
};

// #region context-setup-react
export function App({ config, fallback, children }: AppProps = {}) {
  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) {
    return <>{fallback ?? <p>Loading...</p>}</>;
  }

  const resolvedConfig = defaultConfig(secret, config);
  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
      {children}
    </JazzProvider>
  );
}
// #endregion context-setup-react
