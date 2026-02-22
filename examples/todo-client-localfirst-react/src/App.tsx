import {
  JazzProvider,
  SyntheticUserSwitcher,
  getActiveSyntheticAuth,
  type JazzProviderProps,
} from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

// #region context-setup-react
function defaultConfig(
  overrides: Partial<JazzProviderProps["config"]> = {},
): NonNullable<JazzProviderProps["config"]> {
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

export function App({ config, fallback }: Partial<JazzProviderProps> = {}) {
  const resolvedConfig = defaultConfig(config);

  return (
    <>
      <SyntheticUserSwitcher appId={resolvedConfig.appId} defaultMode="demo" />
      <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
        <h1>Todos</h1>
        <TodoList />
      </JazzProvider>
    </>
  );
}
