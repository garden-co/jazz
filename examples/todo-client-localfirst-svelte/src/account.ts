import type { JazzClientConfig } from "jazz-tools/svelte";

/**
 * The app's backend serves Jazz on this origin (`/jazz`), so the client needs
 * no URL. Tests point the app at a sync server by its app id; a browser client
 * takes that as its `scope` (it names no app of its own).
 */
export type SessionOverrides = Omit<Partial<JazzClientConfig>, "appId"> & { appId?: string };

export function sessionConfig({ appId, ...rest }: SessionOverrides = {}): JazzClientConfig {
  const overrides = appId === undefined ? rest : { ...rest, scope: appId };
  return {
    env: "dev",
    // The local-first client: IndexedDB persistence, works offline.
    client: () => import("jazz-tools/local-first"),
    ...overrides,
    // Start as a device account: no login UI, no auth server.
    initial: "device",
  };
}
