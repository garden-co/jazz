import type { JazzClientConfig } from "jazz-tools/client";

/**
 * A device account, like the todo example: no sign-up, the browser profile
 * holds the account's key. Crew join shows through invite links.
 */
/**
 * Tests point the app at a sync server by its app id; a browser client takes
 * that as its `scope` (it names no app of its own).
 */
export type SessionOverrides = Omit<Partial<JazzClientConfig>, "appId"> & { appId?: string };

export function sessionConfig({ appId, ...rest }: SessionOverrides = {}): JazzClientConfig {
  const overrides = appId === undefined ? rest : { ...rest, scope: appId };
  // The app's backend serves Jazz on this origin (`/jazz`), so it needs no URL.
  return {
    env: "dev",
    // Start on the thin client; load the local-first one in its own chunk
    // once the page has loaded, then keep working offline.
    client: () => import("jazz-tools/local-first"),
    ...overrides,
    initial: "device",
  };
}
