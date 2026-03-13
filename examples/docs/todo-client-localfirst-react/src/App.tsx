import * as React from "react";
import { JazzProvider } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { AuthSessionExamples } from "./AuthSessionExamples.js";
import { TodoList } from "./TodoList.js";

// Keep docs-only auth snippets in the compiled example app.
void AuthSessionExamples;

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

function defaultConfig(overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId: readEnvAppId() ?? "todo-react-example",
    env: "dev",
    userBranch: "main",
    ...overrides,
  };
}

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
};

// #region context-setup-react
export function App({ config, fallback }: AppProps = {}) {
  const resolvedConfig = defaultConfig(config);
  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react
