import type { JazzSessionConfig } from "jazz-tools/client";

export function sessionConfig(overrides: Partial<JazzSessionConfig> = {}): JazzSessionConfig {
  const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
  const appId = overrides.appId ?? env?.VITE_JAZZ_APP_ID ?? env?.JAZZ_APP_ID;
  const serverUrl = overrides.serverUrl ?? env?.VITE_JAZZ_SERVER_URL ?? env?.JAZZ_SERVER_URL;
  if (!appId) throw new Error("Missing Jazz appId");
  if (!serverUrl) throw new Error("Missing Jazz core serverUrl for account preparation");
  return { env: "dev", ...overrides, appId, serverUrl, initial: "local-first" };
}
