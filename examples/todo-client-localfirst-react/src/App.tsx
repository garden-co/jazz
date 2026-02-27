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

const jazzClient = await createJazzClient(defaultConfig());

if (location.origin.includes("localhost")) {
  Object.defineProperty(window, "jazzClient", {
    value: jazzClient,
    writable: true,
  });
}

// #region context-setup-react
export function App() {
  return (
    <JazzProvider client={jazzClient}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react
