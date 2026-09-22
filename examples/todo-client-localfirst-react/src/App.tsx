import * as React from "react";
import { JazzProvider, JazzSessionProvider } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { TodoList } from "./TodoList.js";
import { sessionConfig } from "./account.js";

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
  children?: React.ReactNode;
};

// #region context-setup-react
function LocalFirstApp({ config, fallback, children }: AppProps) {
  return (
    <JazzSessionProvider config={sessionConfig(config)} fallback={fallback ?? <p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
      {children}
    </JazzSessionProvider>
  );
}
// #endregion context-setup-react

export function App(props: AppProps = {}) {
  // Advanced callers can still supply a fixed opaque handle, for example to
  // exercise two independent replicas in the browser integration tests.
  const { config, fallback, children } = props;
  if (config?.account)
    return (
      <JazzProvider
        config={{ appId: config.appId!, env: "dev", ...config, account: config.account }}
        fallback={fallback ?? <p>Loading...</p>}
      >
        <h1>Todos</h1>
        <TodoList />
        {children}
      </JazzProvider>
    );
  return <LocalFirstApp {...props} />;
}
