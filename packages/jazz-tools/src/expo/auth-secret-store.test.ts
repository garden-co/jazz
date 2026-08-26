import { describe, expect, it, vi } from "vitest";

vi.mock("expo-crypto", () => ({ getRandomBytes: vi.fn() }));
vi.mock("expo-secure-store", () => ({
  deleteItemAsync: vi.fn(),
  getItemAsync: vi.fn(),
  setItemAsync: vi.fn(),
}));
import { ExpoAuthSecretStore, type ExpoSecureStoreLike } from "./auth-secret-store.js";

function recordingStore() {
  const getItemAsync = vi.fn<ExpoSecureStoreLike["getItemAsync"]>(async () => null);
  const setItemAsync = vi.fn<ExpoSecureStoreLike["setItemAsync"]>(async () => {});
  const deleteItemAsync = vi.fn<ExpoSecureStoreLike["deleteItemAsync"]>(async () => {});
  const secureStore: ExpoSecureStoreLike = { getItemAsync, setItemAsync, deleteItemAsync };
  return { secureStore, getItemAsync, setItemAsync, deleteItemAsync };
}

describe("ExpoAuthSecretStore scoped keys", () => {
  it("uses only Expo SecureStore-compatible characters for scoped defaults", async () => {
    const { secureStore, setItemAsync } = recordingStore();
    const store = new ExpoAuthSecretStore({
      secureStore,
      appId: "app:one/%",
      userId: "user@example.com",
      sessionId: "session/東京",
    });

    await store.saveSecret("secret");

    const key = setItemAsync.mock.calls[0]?.[0];
    expect(key).toMatch(/^[A-Za-z0-9._-]+$/);
    expect(key).not.toContain(":");
    expect(key).not.toContain("%");
  });

  it("keeps app, user, and session scopes distinct", async () => {
    const { secureStore, getItemAsync } = recordingStore();

    await new ExpoAuthSecretStore({ secureStore, appId: "same" }).loadSecret();
    await new ExpoAuthSecretStore({ secureStore, userId: "same" }).loadSecret();
    await new ExpoAuthSecretStore({ secureStore, sessionId: "same" }).loadSecret();

    const keys = getItemAsync.mock.calls.map(([key]) => key);
    expect(new Set(keys)).toHaveProperty("size", 3);
  });

  it("preserves the unscoped default key", async () => {
    const { secureStore, getItemAsync } = recordingStore();

    await new ExpoAuthSecretStore({ secureStore }).loadSecret();

    expect(getItemAsync).toHaveBeenCalledWith("jazz-auth-secret");
  });
});
