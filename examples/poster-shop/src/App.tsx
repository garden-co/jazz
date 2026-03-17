import { useMemo } from "react";
import { createJazzClient, JazzProvider, getActiveSyntheticAuth } from "jazz-tools/react";
import { Canvas } from "./Canvas.js";

type JazzProviderClientConfig = NonNullable<Parameters<typeof createJazzClient>[0]>;

function readEnvAppId(): string | undefined {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
  return env?.VITE_JAZZ_APP_ID ?? env?.JAZZ_APP_ID;
}

function readEnvServerUrl(): string | undefined {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
  return env?.VITE_JAZZ_SERVER_URL ?? env?.JAZZ_SERVER_URL;
}

function readEnvAdminSecret(): string | undefined {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
  return env?.VITE_JAZZ_ADMIN_SECRET ?? env?.JAZZ_ADMIN_SECRET;
}

function defaultConfig(
  overrides: Partial<JazzProviderClientConfig> = {},
): JazzProviderClientConfig {
  const appId = overrides.appId ?? readEnvAppId() ?? "019cba1b-f59e-7a51-a88b-e1ab571cc672";
  const serverUrl = overrides.serverUrl ?? readEnvServerUrl() ?? "http://127.0.0.1:1625";
  const adminSecret = overrides.adminSecret ?? readEnvAdminSecret();
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });

  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl,
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
    adminSecret,
    ...overrides,
  };
}

export function App() {
  const resolvedConfig = useMemo(() => defaultConfig(), []);
  return (
    <JazzProvider config={resolvedConfig} fallback={<main>Initializing Jazz client...</main>}>
      <main>
        <h1>Poster Shop</h1>
        <Canvas />
      </main>
    </JazzProvider>
  );
}
