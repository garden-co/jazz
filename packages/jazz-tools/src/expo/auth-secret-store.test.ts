import { describe, expect, it, vi } from "vitest";

vi.mock("expo-crypto", () => ({ getRandomBytes: vi.fn() }));
vi.mock("expo-secure-store", () => ({
  deleteItemAsync: vi.fn(),
  getItemAsync: vi.fn(),
  setItemAsync: vi.fn(),
}));
import { ExpoAuthSecretStore, type ExpoSecureStoreLike } from "./auth-secret-store.js";
import {
  authSecretStorageKey,
  generateAuthSecret,
  parseAuthSecret,
} from "../runtime/auth-secret-store.js";

function recordingStore() {
  const getItemAsync = vi.fn<ExpoSecureStoreLike["getItemAsync"]>(async () => null);
  const setItemAsync = vi.fn<ExpoSecureStoreLike["setItemAsync"]>(async () => {});
  const deleteItemAsync = vi.fn<ExpoSecureStoreLike["deleteItemAsync"]>(async () => {});
  const secureStore: ExpoSecureStoreLike = { getItemAsync, setItemAsync, deleteItemAsync };
  return { secureStore, getItemAsync, setItemAsync, deleteItemAsync };
}

describe("ExpoAuthSecretStore scoped keys", () => {
  it("uses the same hashed, SecureStore-compatible key as browser storage", async () => {
    const { secureStore, setItemAsync } = recordingStore();
    const store = new ExpoAuthSecretStore({
      secureStore,
      appId: "app:one/%",
      profile: "user@example.com",
    });

    await store.saveSecret(generateAuthSecret());

    const key = setItemAsync.mock.calls[0]?.[0];
    expect(key).toMatch(/^[A-Za-z0-9._-]+$/);
    expect(key).not.toContain(":");
    expect(key).not.toContain("%");
    expect(key).toBe(authSecretStorageKey({ appId: "app:one/%", profile: "user@example.com" }));
  });

  it("keeps app/profile scopes distinct", async () => {
    const { secureStore, getItemAsync } = recordingStore();

    await new ExpoAuthSecretStore({ secureStore, appId: "same" }).loadSecret();
    await new ExpoAuthSecretStore({ secureStore, profile: "same" }).loadSecret();
    await new ExpoAuthSecretStore({ secureStore, appId: "same", profile: "same" }).loadSecret();

    const keys = getItemAsync.mock.calls.map(([key]) => key);
    expect(new Set(keys)).toHaveProperty("size", 3);
  });

  it("uses the common hashed default key", async () => {
    const { secureStore, getItemAsync } = recordingStore();

    await new ExpoAuthSecretStore({ secureStore }).loadSecret();

    expect(getItemAsync).toHaveBeenCalledWith(authSecretStorageKey());
  });

  it("loads the fixed browser/recovery secret representation byte-for-byte", async () => {
    const { secureStore, setItemAsync } = recordingStore();
    const secret = "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const store = new ExpoAuthSecretStore({ secureStore, appId: "shared-app", profile: "default" });

    await store.saveSecret(secret);
    expect(setItemAsync).toHaveBeenCalledWith(
      authSecretStorageKey({ appId: "shared-app", profile: "default" }),
      secret,
    );
    expect(parseAuthSecret(secret)).toEqual(new Uint8Array(32));
  });

  it("rejects malformed stored secrets before returning them to native auth", async () => {
    const { secureStore } = recordingStore();
    secureStore.getItemAsync = async () =>
      "jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const store = new ExpoAuthSecretStore({ secureStore });
    await expect(store.loadSecret()).rejects.toThrow(/43 unpadded/);
  });
});
