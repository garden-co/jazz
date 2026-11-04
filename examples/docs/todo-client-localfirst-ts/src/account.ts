import type { JazzClientConfig } from "jazz-tools/client";

/** The app's backend serves Jazz on this origin (`/jazz`), so the client needs no URL. */
export function sessionConfig(overrides: Partial<JazzClientConfig> = {}): JazzClientConfig {
  return { env: "dev", ...overrides, initial: "local-first" };
}
