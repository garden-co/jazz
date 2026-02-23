import {
  JazzProvider,
  SyntheticUserSwitcher,
  getActiveSyntheticAuth,
  type JazzProviderProps,
} from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

function readEnvVar(name: string): string | undefined {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
  return env?.[name];
}

function readEnvAppId(): string | undefined {
  return readEnvVar("JAZZ_APP_ID") ?? readEnvVar("VITE_JAZZ_APP_ID");
}

function readEnvServerUrl(): string | undefined {
  return readEnvVar("JAZZ_SERVER_URL") ?? readEnvVar("VITE_JAZZ_SERVER_URL");
}

function readEnvAdminSecret(): string | undefined {
  return readEnvVar("JAZZ_ADMIN_SECRET") ?? readEnvVar("VITE_JAZZ_ADMIN_SECRET");
}

function readEnvLocalMode(): "anonymous" | "demo" | undefined {
  const value = readEnvVar("JAZZ_LOCAL_MODE") ?? readEnvVar("VITE_JAZZ_LOCAL_MODE");
  return value === "anonymous" || value === "demo" ? value : undefined;
}

function readEnvLocalToken(): string | undefined {
  return readEnvVar("JAZZ_LOCAL_TOKEN") ?? readEnvVar("VITE_JAZZ_LOCAL_TOKEN");
}

// #region context-setup-react
function defaultConfig(
  overrides: Partial<JazzProviderProps["config"]> = {},
): NonNullable<JazzProviderProps["config"]> {
  const appId = overrides.appId ?? readEnvAppId() ?? "00000000-0000-0000-0000-000000000002";
  const serverUrl = overrides.serverUrl ?? readEnvServerUrl() ?? "http://127.0.0.1:1625";
  const adminSecret = overrides.adminSecret ?? readEnvAdminSecret() ?? "dev-admin-secret";
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });
  const localAuthMode = overrides.localAuthMode ?? readEnvLocalMode() ?? active.localAuthMode;
  const localAuthToken = overrides.localAuthToken ?? readEnvLocalToken() ?? active.localAuthToken;

  return {
    appId,
    serverUrl,
    adminSecret,
    env: "dev",
    userBranch: "main",
    localAuthMode,
    localAuthToken,
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
