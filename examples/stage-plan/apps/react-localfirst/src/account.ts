import type { JazzSessionConfig } from "jazz-tools/client";

/**
 * A local-first account, like the todo example: no sign-up, the browser
 * profile holds the account secret. Crew join shows through invite links.
 */
export function sessionConfig(overrides: Partial<JazzSessionConfig> = {}): JazzSessionConfig {
  const env = import.meta.env as Record<string, string | undefined>;
  const appId = overrides.appId ?? env.VITE_JAZZ_APP_ID ?? env.JAZZ_APP_ID;
  const serverUrl = overrides.serverUrl ?? env.VITE_JAZZ_SERVER_URL ?? env.JAZZ_SERVER_URL;
  if (!appId) throw new Error("Missing Jazz appId");
  if (!serverUrl) throw new Error("Missing Jazz serverUrl");
  return { env: "dev", ...overrides, appId, serverUrl, initial: "local-first" };
}
