/** Shared constants for browser tests — no Node.js imports. */
export const EDGE_TEST_PORT = 19877;
export const CORE_TEST_PORT = 19891;
export const TEST_PORT = EDGE_TEST_PORT;
export const EDGE_SERVER_URL = `http://127.0.0.1:${EDGE_TEST_PORT}`;
export const CORE_SERVER_URL = `http://127.0.0.1:${CORE_TEST_PORT}`;
export const JWT_SECRET = "test-jwt-secret-for-react-browser-tests";
export const ADMIN_SECRET = "test-admin-secret-for-react-browser-tests";
export const PEER_SECRET = "test-peer-secret-for-react-browser-tests";
export const APP_ID = "019d4349-24b0-72a9-ae86-8ed24a7e3a90";
