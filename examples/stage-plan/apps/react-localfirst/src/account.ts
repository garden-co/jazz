import type { JazzSessionConfig } from "jazz-tools/client";

/**
 * A local-first account, like the todo example: no sign-up, the browser
 * profile holds the account secret. Crew join shows through invite links.
 */
export function sessionConfig(overrides: Partial<JazzSessionConfig> = {}): JazzSessionConfig {
  // The app's backend serves Jazz on this origin (`/jazz`), so it needs no URL.
  return {
    env: "dev",
    // Start on the thin client; load the local-first one in its own chunk
    // once the page has loaded, then keep working offline.
    client: () => import("jazz-tools/local-first"),
    ...overrides,
    initial: "local-first",
  };
}
