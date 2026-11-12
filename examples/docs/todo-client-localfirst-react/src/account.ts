import type { JazzClientConfig } from "jazz-tools/client";

/**
 * The app's backend serves Jazz on this origin (`/jazz`), so the client needs
 * no URL. Tests point the app at a sync server by its app id; a browser client
 * takes that as its `scope` (it names no app of its own).
 */
export type SessionOverrides = Omit<Partial<JazzClientConfig>, "appId"> & { appId?: string };

/** Everyone starts as a guest: a device account on this device. */
export function sessionConfig({ appId, ...rest }: SessionOverrides = {}): JazzClientConfig {
  const overrides = appId === undefined ? rest : { ...rest, scope: appId };
  return {
    env: "dev",
    initial: "device",
    // Local data, so the app works offline once it has loaded.
    client: () => import("jazz-tools/local-first"),
    ...overrides,
  };
}
