import * as React from "react";
import { JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";
import { sessionConfig, type SessionOverrides } from "./account.js";

type AppProps = {
  config?: SessionOverrides;
  fallback?: React.ReactNode;
  children?: React.ReactNode;
};

// #region context-setup-react
export function App({ config, fallback, children }: AppProps = {}) {
  return (
    <JazzProvider config={sessionConfig(config)} loading={fallback ?? <p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
      {children}
    </JazzProvider>
  );
}
// #endregion context-setup-react
