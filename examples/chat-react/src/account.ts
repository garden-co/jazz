import type { JazzClientConfig } from "jazz-tools";

/** Tests may name an app id (taken as the client's `scope`) and pass session options. */
export type AppConfig = Omit<Partial<JazzClientConfig>, "appId"> & { appId?: string };

/**
 * The app's session: every visitor starts as a device account (no login), on
 * the local-first client. The app's backend serves Jazz on this origin
 * (`/jazz`, the default).
 */
export function sessionConfig({ appId, ...overrides }: AppConfig = {}): JazzClientConfig {
  return {
    env: "dev",
    client: () => import("jazz-tools/local-first"),
    localFirst: "now",
    ...overrides,
    ...(appId === undefined ? {} : { scope: appId }),
    initial: "device",
  };
}
