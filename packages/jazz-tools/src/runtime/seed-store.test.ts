import { describe, it, expect, beforeEach } from "vitest";
import { LocalStorageSeedStore, generateSeed } from "./seed-store.js";

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

describe("generateSeed", () => {
  it("produces a base64url string", () => {
    const seed = generateSeed();
    // 32 bytes → 43 base64url chars (no padding)
    expect(seed).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });

  it("produces different seeds each call", () => {
    const a = generateSeed();
    const b = generateSeed();
    expect(a).not.toBe(b);
  });
});

describe("LocalStorageSeedStore", () => {
  let storage: Pick<Storage, "getItem" | "setItem" | "removeItem">;
  let store: LocalStorageSeedStore;

  beforeEach(() => {
    storage = createMockStorage();
    store = new LocalStorageSeedStore({ storage });
  });

  it("loadSeed returns null when no seed stored", async () => {
    expect(await store.loadSeed()).toBeNull();
  });

  it("saveSeed persists and loadSeed retrieves", async () => {
    const seed = generateSeed();
    await store.saveSeed(seed);
    expect(await store.loadSeed()).toBe(seed);
  });

  it("clearSeed removes the seed", async () => {
    await store.saveSeed(generateSeed());
    await store.clearSeed();
    expect(await store.loadSeed()).toBeNull();
  });

  it("getOrCreateSeed generates on first call", async () => {
    const seed = await store.getOrCreateSeed();
    expect(seed).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });

  it("getOrCreateSeed returns same seed on second call", async () => {
    const first = await store.getOrCreateSeed();
    const second = await store.getOrCreateSeed();
    expect(first).toBe(second);
  });

  it("clearSeed then getOrCreateSeed produces a new seed", async () => {
    const first = await store.getOrCreateSeed();
    await store.clearSeed();
    const second = await store.getOrCreateSeed();
    expect(second).not.toBe(first);
  });

  it("uses custom key name", async () => {
    const customStore = new LocalStorageSeedStore({ storage, key: "my-custom-key" });
    await customStore.saveSeed("test-seed");
    expect(storage.getItem("my-custom-key")).toBe("test-seed");
  });

  it("default key is jazz-seed", async () => {
    await store.saveSeed("test-seed");
    expect(storage.getItem("jazz-seed")).toBe("test-seed");
  });
});
