import type { JazzClientConfig } from "jazz-tools/svelte";

/**
 * The app's backend serves Jazz on this origin (`/jazz`), so the client needs
 * no URL. Everyone starts as a guest: a device account on this device.
 */
export function sessionConfig(overrides: Partial<JazzClientConfig> = {}): JazzClientConfig {
  return { env: "dev", initial: "device", ...overrides };
}
