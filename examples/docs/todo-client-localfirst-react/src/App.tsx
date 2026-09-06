import * as React from "react";
import { JazzProvider } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { AuthSessionExamples } from "./AuthSessionExamples.js";
void AuthSessionExamples;
import { TodoList } from "./TodoList.js";
import { prepareAccountConfig } from "./account.js";

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
  children?: React.ReactNode;
};

// #region context-setup-react
export function App({ config, fallback, children }: AppProps = {}) {
  const [resolved, setResolved] = React.useState<DbConfig>();
  const [error, setError] = React.useState<Error>();
  React.useEffect(() => {
    let cancelled = false;
    // Keep the active provider while preparing. Equivalent inline configs
    // retain the provider registry key and must not tear down its context.
    setError(undefined);
    prepareAccountConfig(config).then(
      (value) => {
        if (!cancelled) setResolved(value);
      },
      (cause) => {
        if (!cancelled) setError(cause instanceof Error ? cause : new Error(String(cause)));
      },
    );
    return () => {
      cancelled = true;
    };
  }, [config]);
  if (error) throw error;
  if (!resolved) return <>{fallback ?? <p>Loading...</p>}</>;
  return (
    <JazzProvider config={resolved} fallback={fallback ?? <p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
      {children}
    </JazzProvider>
  );
}
// #endregion context-setup-react
