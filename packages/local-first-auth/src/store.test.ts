import { describe, expect, it } from "vitest";
import { browserSecretStore } from "./browser.js";
import {
  createSecretStore,
  generateSecret,
  legacyJazzStorageKey,
  type SecretStorage,
} from "./index.js";

function memoryStorage(
  initial: Record<string, string> = {},
): SecretStorage & { map: Map<string, string> } {
  const map = new Map(Object.entries(initial));
  return {
    map,
    getItem: (key) => map.get(key) ?? null,
    setItem: (key, value) => void map.set(key, value),
    removeItem: (key) => void map.delete(key),
  };
}

describe("createSecretStore", () => {
  it("creates once and then returns the stored secret", async () => {
    const storage = memoryStorage();
    const store = createSecretStore({ key: "/jazz", storage });
    expect(await store.load()).toBeNull();
    const [a, b] = await Promise.all([store.getOrCreate(), store.getOrCreate()]);
    expect(a).toBe(b);
    expect(storage.map.get("/jazz")).toBe(a);
  });

  it("shares one instance per storage and key", () => {
    const storage = memoryStorage();
    expect(createSecretStore({ key: "/jazz", storage })).toBe(
      createSecretStore({ key: "/jazz", storage }),
    );
    expect(createSecretStore({ key: "/a", storage })).not.toBe(
      createSecretStore({ key: "/b", storage }),
    );
  });

  it("adopts a secret from a legacy Jazz key and leaves the old entry", async () => {
    const legacy = legacyJazzStorageKey("chat-app");
    const old = generateSecret();
    const storage = memoryStorage({ [legacy]: old });
    const store = createSecretStore({ key: "/jazz", storage, migrateFrom: [legacy] });
    expect(await store.getOrCreate()).toBe(old);
    expect(storage.map.get("/jazz")).toBe(old);
    expect(storage.map.get(legacy)).toBe(old);
  });

  it("prefers the current key over legacy keys", async () => {
    const legacy = legacyJazzStorageKey("chat-app");
    const current = generateSecret();
    const storage = memoryStorage({ "/jazz": current, [legacy]: generateSecret() });
    const store = createSecretStore({ key: "/jazz", storage, migrateFrom: [legacy] });
    expect(await store.load()).toBe(current);
  });

  it("rejects malformed stored secrets and invalid saves", async () => {
    const store = createSecretStore({ key: "/jazz", storage: memoryStorage({ "/jazz": "nope" }) });
    await expect(store.load()).rejects.toThrow(/jazz-auth-v1/);
    await expect(store.save("nope")).rejects.toThrow();
  });

  it("notifies subscribers on save and clear", async () => {
    const store = createSecretStore({ key: "/jazz", storage: memoryStorage() });
    const states: (string | null)[] = [];
    const unsubscribe = store.subscribe(() => {
      const state = store.getState();
      if (!state.isLoading) states.push(state.secret);
    });
    await new Promise((r) => setTimeout(r, 0));
    const restored = generateSecret();
    await store.save(restored);
    await new Promise((r) => setTimeout(r, 0));
    expect(states.at(-1)).toBe(restored);
    await store.clear();
    await new Promise((r) => setTimeout(r, 0));
    // A cleared store creates a fresh secret for its subscribers.
    expect(states.at(-1)).not.toBe(restored);
    expect(states).toHaveLength(3);
    unsubscribe();
  });
});

describe("browserSecretStore", () => {
  it("fails clearly without localStorage", async () => {
    await expect(browserSecretStore({ key: "/nowhere" }).load()).rejects.toThrow(/localStorage/);
  });
});
