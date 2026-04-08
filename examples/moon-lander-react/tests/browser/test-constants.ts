/** Shared constants for browser tests — no Node imports. */

/** Injected by vitest.config.browser.ts via `define`. */
declare const __TEST_PORT__: number;

export const TEST_PORT = __TEST_PORT__;
export const ADMIN_SECRET = "test-admin-secret-for-moon-lander-tests";
export const APP_ID = "00000000-0000-0000-0000-000000000003";
/** Separate app namespace for multi-player tests so they start with an empty
 *  event history — preventing stream connect timeouts in the isolated
 *  BrowserContext caused by accumulated events from earlier tests. */
export const APP_ID_MULTI = "00000000-0000-0000-0000-000000000004";
