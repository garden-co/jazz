import { describe, it, expect, beforeEach } from "vitest";
import {
  authSecretStorageKey,
  BrowserAuthSecretStore,
  generateAuthSecret,
  parseAuthSecret,
} from "./auth-secret-store.js";

function createMockStorage(): Pick<Storage, "getItem" | "setItem" | "removeItem"> {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => {
      store.set(key, value);
    },
    removeItem: (key: string) => {
      store.delete(key);
    },
  };
}

describe("generateAuthSecret", () => {
  it("produces the versioned canonical 256-bit representation", () => {
    const secret = generateAuthSecret();
    expect(secret).toMatch(/^jazz-auth-v1:[A-Za-z0-9_-]{43}$/);
    expect(parseAuthSecret(secret)).toHaveLength(32);
  });

  it("produces different secrets each call", () => {
    const a = generateAuthSecret();
    const b = generateAuthSecret();
    expect(a).not.toBe(b);
  });

  it("rejects unversioned, padded, and noncanonical payloads", () => {
    expect(() => parseAuthSecret("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")).toThrow(
      /jazz-auth-v1/,
    );
    expect(() =>
      parseAuthSecret("jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    ).toThrow(/43 unpadded/);
    // The final base64url sextet has unused bits for a 32-byte payload. This
    // different spelling decodes to the same bytes but must not be accepted.
    expect(() =>
      parseAuthSecret("jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB"),
    ).toThrow(/canonical/);
  });
});

describe("BrowserAuthSecretStore", () => {
  let storage: Pick<Storage, "getItem" | "setItem" | "removeItem">;
  let store: BrowserAuthSecretStore;

  beforeEach(() => {
    storage = createMockStorage();
    store = new BrowserAuthSecretStore({ storage });
  });

  it("loadSecret returns null when no secret stored", async () => {
    expect(await store.loadSecret()).toBeNull();
  });

  it("saveSecret persists and loadSecret retrieves", async () => {
    const secret = generateAuthSecret();
    await store.saveSecret(secret);
    expect(await store.loadSecret()).toBe(secret);
  });

  it("clearSecret removes the secret", async () => {
    await store.saveSecret(generateAuthSecret());
    await store.clearSecret();
    expect(await store.loadSecret()).toBeNull();
  });

  it("getOrCreateSecret generates on first call", async () => {
    const secret = await store.getOrCreateSecret();
    expect(secret).toMatch(/^jazz-auth-v1:[A-Za-z0-9_-]{43}$/);
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

  it("uses a custom physical key name", async () => {
    const customStore = new BrowserAuthSecretStore({ storage, key: "my-custom-key" });
    const secret = generateAuthSecret();
    await customStore.saveSecret(secret);
    expect(storage.getItem("my-custom-key")).toBe(secret);
  });

  it("uses a versioned hashed default key", async () => {
    const secret = generateAuthSecret();
    await store.saveSecret(secret);
    expect(storage.getItem(authSecretStorageKey())).toBe(secret);
  });

  it("saveSecret updates getOrCreateSecret's cache", async () => {
    const first = await store.getOrCreateSecret();
    const replacement = generateAuthSecret();
    expect(replacement).not.toBe(first);
    await store.saveSecret(replacement);
    expect(await store.getOrCreateSecret()).toBe(replacement);
  });

  it("saveSecret updates loadSecret even after getOrCreateSecret was cached", async () => {
    await store.getOrCreateSecret();
    const replacement = generateAuthSecret();
    await store.saveSecret(replacement);
    expect(await store.loadSecret()).toBe(replacement);
  });

  it("isolates secrets by appId/profile without putting raw scope values in keys", async () => {
    const aliceStore = new BrowserAuthSecretStore({
      storage,
      appId: "chat-app",
      profile: "alice@example.com",
    });
    const bobStore = new BrowserAuthSecretStore({
      storage,
      appId: "chat-app",
      profile: "bob@example.com",
    });
    const aliceAgainStore = new BrowserAuthSecretStore({
      storage,
      appId: "chat-app",
      profile: "alice@example.com",
    });

    const aliceSecret = await aliceStore.getOrCreateSecret();
    const bobSecret = await bobStore.getOrCreateSecret();
    const aliceAgainSecret = await aliceAgainStore.getOrCreateSecret();

    expect(aliceSecret).not.toBe(bobSecret);
    expect(aliceAgainSecret).toBe(aliceSecret);
  });

  it("rejects malformed persisted and restored values before they reach auth", async () => {
    storage.setItem(authSecretStorageKey(), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    await expect(store.loadSecret()).rejects.toThrow(/jazz-auth-v1/);
    await expect(
      store.saveSecret("jazz-auth-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    ).rejects.toThrow(/43 unpadded/);
  });

  it("throws a clear error if used in a non-browser env (no localStorage)", async () => {
    const original = (globalThis as { localStorage?: Storage }).localStorage;
    delete (globalThis as { localStorage?: Storage }).localStorage;
    try {
      const ssrStore = new BrowserAuthSecretStore();
      expect(() => ssrStore.getOrCreateSecret()).toThrow(/browser environment/);
      await expect(ssrStore.loadSecret()).rejects.toThrow(/browser environment/);
    } finally {
      if (original !== undefined) {
        (globalThis as { localStorage?: Storage }).localStorage = original;
      }
    }
  });

  it("static helpers can isolate secrets by namespace hints", async () => {
    const aliceSecret = await BrowserAuthSecretStore.getOrCreateSecret({
      storage,
      appId: "docs-chat",
      profile: "alice",
    });
    const bobSecret = await BrowserAuthSecretStore.getOrCreateSecret({
      storage,
      appId: "docs-chat",
      profile: "bob",
    });
    const aliceAgainSecret = await BrowserAuthSecretStore.getOrCreateSecret({
      storage,
      appId: "docs-chat",
      profile: "alice",
    });

    expect(aliceSecret).not.toBe(bobSecret);
    expect(aliceAgainSecret).toBe(aliceSecret);
  });

  it("has a stable cross-platform scope-key fixture", () => {
    expect(authSecretStorageKey({ appId: "band-chat", profile: "default" })).toBe(
      "jazz-auth-store-v1-xtjm7t8x0cqlJ-wMiSiH7DwnziSS30JOh-Op-rlyVWE",
    );
  });

  it("keeps distinct app/profile identifiers distinct byte-for-byte", () => {
    expect(authSecretStorageKey({ appId: "band-chat", profile: "default" })).not.toBe(
      authSecretStorageKey({ appId: " band-chat", profile: "default" }),
    );
    expect(authSecretStorageKey({ appId: "café" })).not.toBe(
      authSecretStorageKey({ appId: "cafe\u0301" }),
    );
    expect(authSecretStorageKey({ profile: "" })).not.toBe(authSecretStorageKey());
  });

  it("rejects non-string untyped scope values instead of silently colliding", () => {
    expect(() => authSecretStorageKey({ appId: 42 as unknown as string })).toThrow(
      /appId must be a string or null/,
    );
  });
});
