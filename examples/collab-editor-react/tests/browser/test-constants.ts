/** Shared constants for browser tests — no Node.js imports. */
export const EDGE_TEST_PORT = 19878;
export const CORE_TEST_PORT = 19892;
export const TEST_PORT = EDGE_TEST_PORT;
export const EDGE_SERVER_URL = `http://127.0.0.1:${EDGE_TEST_PORT}`;
export const CORE_SERVER_URL = `http://127.0.0.1:${CORE_TEST_PORT}`;
export const JWT_SECRET = "test-jwt-secret-for-collab-editor-browser-tests";
export const ADMIN_SECRET = "test-admin-secret-for-collab-editor-browser-tests";
export const APP_ID = "b9268e28-8dc4-45fe-9a50-f56df7cc75ef";
