import type { JazzSessionConfig } from "jazz-tools/client";

/** The app's backend serves Jazz on this origin (`/jazz`), so the client needs no URL. */
/**
 * Tests point the app at a sync server by its app id; a browser client takes
 * that as its `scope` (it names no app of its own).
 */
type SessionOverrides = Omit<Partial<JazzSessionConfig>, "appId"> & { appId?: string };

export function sessionConfig({ appId, ...rest }: SessionOverrides = {}): JazzSessionConfig {
  const overrides = appId === undefined ? rest : { ...rest, scope: appId };
  return { env: "dev", ...overrides, initial: "local-first" };
}
