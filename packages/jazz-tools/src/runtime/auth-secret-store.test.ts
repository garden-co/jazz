import { describe, it, expect, beforeEach } from "vitest";
import { BrowserAuthSecretStore, generateAuthSecret } from "./auth-secret-store.js";

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
  it("produces a base64url string", () => {
    const secret = generateAuthSecret();
    // 32 bytes → 43 base64url chars (no padding)
    expect(secret).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });

  it("produces different secrets each call", () => {
    const a = generateAuthSecret();
    const b = generateAuthSecret();
    expect(a).not.toBe(b);
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
    const customStore = new BrowserAuthSecretStore({ storage, key: "my-custom-key" });
    await customStore.saveSecret("test-secret");
    expect(storage.getItem("my-custom-key")).toBe("test-secret");
  });

  it("default key is jazz-auth-secret", async () => {
    await store.saveSecret("test-secret");
    expect(storage.getItem("jazz-auth-secret")).toBe("test-secret");
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

  it("isolates secrets by appId/userId when namespace hints are provided", async () => {
    const aliceStore = new BrowserAuthSecretStore({
      storage,
      appId: "chat-app",
      userId: "alice",
    });
    const bobStore = new BrowserAuthSecretStore({
      storage,
      appId: "chat-app",
      userId: "bob",
    });
    const aliceAgainStore = new BrowserAuthSecretStore({
      storage,
      appId: "chat-app",
      userId: "alice",
    });

    const aliceSecret = await aliceStore.getOrCreateSecret();
    const bobSecret = await bobStore.getOrCreateSecret();
    const aliceAgainSecret = await aliceAgainStore.getOrCreateSecret();

    expect(aliceSecret).not.toBe(bobSecret);
    expect(aliceAgainSecret).toBe(aliceSecret);
  });

  it("static helpers can isolate secrets by namespace hints", async () => {
    const aliceSecret = await BrowserAuthSecretStore.getOrCreateSecret({
      storage,
      appId: "docs-chat",
      userId: "alice",
    });
    const bobSecret = await BrowserAuthSecretStore.getOrCreateSecret({
      storage,
      appId: "docs-chat",
      userId: "bob",
    });
    const aliceAgainSecret = await BrowserAuthSecretStore.getOrCreateSecret({
      storage,
      appId: "docs-chat",
      userId: "alice",
    });

    expect(aliceSecret).not.toBe(bobSecret);
    expect(aliceAgainSecret).toBe(aliceSecret);
  });
});
