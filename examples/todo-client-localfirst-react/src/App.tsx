import {
  createJazzClient,
  JazzProvider,
  SyntheticUserSwitcher,
  getActiveSyntheticAuth,
} from "jazz-tools/react";
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

const resolvedConfig = defaultConfig();
const client = createJazzClient(resolvedConfig);

export function App() {
  return (
    <>
      <SyntheticUserSwitcher
        appId={resolvedConfig.appId}
        defaultMode="demo"
        onProfileChange={() => window.location.reload()}
      />
      <JazzProvider client={client}>
        <h1>Todos</h1>
        <TodoList />
      </JazzProvider>
    </>
  );
}
