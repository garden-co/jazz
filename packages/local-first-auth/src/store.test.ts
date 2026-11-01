import { describe, expect, it } from "vitest";
import { browserSecretStore } from "./browser.js";
import {
  createSecretStore,
  generateSecret,
  secretStorageKey,
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
    const store = createSecretStore({ scope: "chat-app", storage });
    expect(await store.load()).toBeNull();
    const [a, b] = await Promise.all([store.getOrCreate(), store.getOrCreate()]);
    expect(a).toBe(b);
    expect(storage.map.get(secretStorageKey("chat-app"))).toBe(a);
  });

  it("shares one instance per storage, scope and profile", () => {
    const storage = memoryStorage();
    expect(createSecretStore({ scope: "a", storage })).toBe(
      createSecretStore({ scope: "a", storage }),
    );
    expect(createSecretStore({ scope: "a", storage })).not.toBe(
      createSecretStore({ scope: "b", storage }),
    );
    expect(createSecretStore({ scope: "a", storage })).not.toBe(
      createSecretStore({ scope: "a", profile: "bob", storage }),
    );
  });

  it("derives the storage key Jazz has always used", () => {
    // Pinned from jazz-tools up to alpha.59; changing it would lose users' secrets.
    expect(secretStorageKey("band-chat", "default")).toBe(
      "jazz-auth-store-v1-xtjm7t8x0cqlJ-wMiSiH7DwnziSS30JOh-Op-rlyVWE",
    );
    expect(secretStorageKey("band-chat")).not.toBe(secretStorageKey(" band-chat"));
    expect(secretStorageKey(null)).not.toBe(secretStorageKey("null"));
  });

  it("finds a secret stored by an alpha.59 Jazz app", async () => {
    const old = generateSecret();
    const storage = memoryStorage({
      "jazz-auth-store-v1-xtjm7t8x0cqlJ-wMiSiH7DwnziSS30JOh-Op-rlyVWE": old,
    });
    const store = createSecretStore({
      scope: "band-chat",
      profile: "default",
      storage,
    });
    expect(await store.getOrCreate()).toBe(old);
  });

  it("rejects malformed stored secrets and invalid saves", async () => {
    const store = createSecretStore({
      storage: memoryStorage({ [secretStorageKey(null)]: "nope" }),
    });
    await expect(store.load()).rejects.toThrow(/jazz-auth-v1/);
    await expect(store.save("nope")).rejects.toThrow();
  });

  it("notifies subscribers on save and clear", async () => {
    const store = createSecretStore({
      scope: "/jazz",
      storage: memoryStorage(),
    });
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
    await expect(browserSecretStore({ scope: "/nowhere" }).load()).rejects.toThrow(/localStorage/);
  });
});
