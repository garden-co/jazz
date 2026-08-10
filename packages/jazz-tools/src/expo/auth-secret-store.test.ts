import { describe, expect, it, vi } from "vitest";

vi.mock("expo-crypto", () => ({
  getRandomBytes: (length: number) => new Uint8Array(length),
}));

vi.mock("expo-secure-store", () => ({
  deleteItemAsync: vi.fn(),
  getItemAsync: vi.fn(),
  setItemAsync: vi.fn(),
}));

import { ExpoAuthSecretStore, type ExpoSecureStoreLike } from "./auth-secret-store.js";

function recordingStore(keys: string[]): ExpoSecureStoreLike {
  return {
    async getItemAsync(key) {
      keys.push(key);
      return null;
    },
    async setItemAsync(key) {
      keys.push(key);
    },
    async deleteItemAsync(key) {
      keys.push(key);
    },
  };
}

describe("ExpoAuthSecretStore", () => {
  it("uses the unscoped SecureStore-compatible default key", async () => {
    const keys: string[] = [];
    await new ExpoAuthSecretStore({ secureStore: recordingStore(keys) }).loadSecret();

    expect(keys).toEqual(["jazz-auth-secret"]);
  });

  it("encodes scoped keys using only Expo SecureStore's accepted alphabet", async () => {
    const keys: string[] = [];
    await new ExpoAuthSecretStore({
      appId: " app/id ",
      userId: "josé",
      sessionId: "session:1",
      secureStore: recordingStore(keys),
    }).loadSecret();

    expect(keys).toEqual([
      "jazz-auth-secret.006100700070002f00690064.006a006f007300e9.00730065007300730069006f006e003a0031",
    ]);
    expect(keys[0]).toMatch(/^[A-Za-z0-9._-]+$/);
  });
});
