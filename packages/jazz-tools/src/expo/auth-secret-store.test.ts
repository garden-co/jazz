import { describe, it, expect, beforeEach } from "vitest";
import { ExpoAuthSecretStore } from "./auth-secret-store.js";

function createMockSecureStore() {
  const store = new Map<string, string>();
  return {
    getItemAsync: (key: string) => Promise.resolve(store.get(key) ?? null),
    setItemAsync: (key: string, value: string) => {
      store.set(key, value);
      return Promise.resolve();
    },
    deleteItemAsync: (key: string) => {
      store.delete(key);
      return Promise.resolve();
    },
  };
}

describe("ExpoAuthSecretStore", () => {
  let secureStore: ReturnType<typeof createMockSecureStore>;
  let store: ExpoAuthSecretStore;

  beforeEach(() => {
    secureStore = createMockSecureStore();
    store = new ExpoAuthSecretStore({ secureStore });
  });

  it("loadSecret returns null when no secret stored", async () => {
    expect(await store.loadSecret()).toBeNull();
  });

  it("saveSecret persists and loadSecret retrieves", async () => {
    const secret = "test-secret-base64url-value-abcdefg";
    await store.saveSecret(secret);
    expect(await store.loadSecret()).toBe(secret);
  });

  it("clearSecret removes the secret", async () => {
    await store.saveSecret("test-secret");
    await store.clearSecret();
    expect(await store.loadSecret()).toBeNull();
  });

  it("getOrCreateSecret generates on first call", async () => {
    const secret = await store.getOrCreateSecret();
    expect(secret).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });

  it("getOrCreateSecret returns same secret on second call", async () => {
    const first = await store.getOrCreateSecret();
    const second = await store.getOrCreateSecret();
    expect(first).toBe(second);
  });

  it("getOrCreateSecret returns the same promise instance", () => {
    const p1 = store.getOrCreateSecret();
    const p2 = store.getOrCreateSecret();
    expect(p1).toBe(p2);
  });

  it("clearSecret then getOrCreateSecret produces a new secret", async () => {
    const first = await store.getOrCreateSecret();
    await store.clearSecret();
    const second = await store.getOrCreateSecret();
    expect(second).not.toBe(first);
  });

  it("uses custom key name", async () => {
    const customStore = new ExpoAuthSecretStore({ secureStore, key: "my-custom-key" });
    await customStore.saveSecret("test-secret");
    expect(await secureStore.getItemAsync("my-custom-key")).toBe("test-secret");
  });

  it("default key is jazz-auth-secret", async () => {
    await store.saveSecret("test-secret");
    expect(await secureStore.getItemAsync("jazz-auth-secret")).toBe("test-secret");
  });
});
