import * as React from "react";
import { JazzProvider, type JazzSession, type JazzClient } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";
import { sessionConfig, type SessionOverrides } from "./account.js";

type AppProps = {
  config?: SessionOverrides;
  /** A session you opened yourself (tests open one to restore a device key). */
  session?: JazzSession<JazzClient>;
  loading?: React.ReactNode;
  children?: React.ReactNode;
};

// #region context-setup-react
function DeviceAccountApp({ config, loading, children }: AppProps) {
  return (
    <JazzProvider config={sessionConfig(config)} loading={loading ?? <p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
      {children}
    </JazzProvider>
  );
}
// #endregion context-setup-react

export function App(props: AppProps = {}) {
  const { session, loading, children } = props;
  if (session)
    return (
      <JazzProvider session={session} loading={loading ?? <p>Loading...</p>}>
        <h1>Todos</h1>
        <TodoList />
        {children}
      </JazzProvider>
    );
  return <DeviceAccountApp {...props} />;
}
