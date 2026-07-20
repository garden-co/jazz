/** Shared constants for browser tests -- no Node.js imports. */
import { inject } from "vitest";

function injectedServerUrl(): string | undefined {
  try {
    return inject("jazzServerUrl");
  } catch {
    return undefined;
  }
}

export const TEST_SERVER_URL =
  injectedServerUrl() ?? import.meta.env.VITE_JAZZ_TEST_SERVER_URL ?? "http://127.0.0.1:19880";
export const TEST_PORT = Number(new URL(TEST_SERVER_URL).port);
export const JWT_SECRET = "test-jwt-secret-for-chat-react-tests";
export const ADMIN_SECRET = "test-admin-secret-for-chat-react-tests";
export const APP_ID = "019d4349-24f1-7053-a5ae-b5fb5600f7a7";
