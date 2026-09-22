/** Shared constants for browser tests -- no Node.js imports. */
import { inject } from "vitest";
import { createAccountManager, type AccountHandle } from "jazz-tools";

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

/**
 * Derive a versioned 32-byte auth secret from a human-readable label.
 * Uses SHA-256 so the result is deterministic and always the right format
 * for local-first authentication.
 */
export async function testAccount(label: string, appId = APP_ID): Promise<AccountHandle> {
  const data = new TextEncoder().encode(label);
  const hash = await crypto.subtle.digest("SHA-256", data);
  const bytes = new Uint8Array(hash);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  const secret =
    "jazz-auth-v1:" + btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  let stored: string | null = null;
  const manager = await createAccountManager({
    appId,
    serverUrl: TEST_SERVER_URL,
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  return manager.getLoggedIn() ?? manager.restoreLocalFirst(secret);
}
