/** Shared constants for browser tests. No Node.js imports. */
import { inject } from "vitest";

export const ADMIN_SECRET = "test-admin-secret-for-stage-plan-browser-tests";
export const APP_ID = "019d6a51-3c1e-7b42-9f0e-5a7c2e8b4d61";

/** The global setup starts the server on a free port and provides its URL. */
export function serverUrl(): string {
  return inject("jazzServerUrl");
}
