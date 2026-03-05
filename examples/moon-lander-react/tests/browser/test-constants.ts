/** Shared constants for browser tests — no Node imports. */

/** Injected by vitest.config.browser.ts via `define`. */
declare const __TEST_PORT__: number;

export const TEST_PORT = __TEST_PORT__;
export const ADMIN_SECRET = "test-admin-secret-for-moon-lander-tests";
export const APP_ID = "00000000-0000-0000-0000-000000000003";
