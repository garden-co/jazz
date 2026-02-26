import * as React from "react";
import { createJazzClient, JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

type JazzProviderClientConfig = NonNullable<Parameters<typeof createJazzClient>[0]>;

function defaultConfig(
  overrides: Partial<JazzProviderClientConfig> = {},
): JazzProviderClientConfig {
  return {
    appId: readEnvAppId() ?? "todo-react-example",
    env: "dev",
    userBranch: "main",
    ...overrides,
  };
}

const client = createJazzClient(defaultConfig());

// #region context-setup-react
export function App() {
  return (
    <JazzProvider client={client}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react
