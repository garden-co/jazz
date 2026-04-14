import { describe, it, expect } from "vitest";
import { BrowserAuthSecretStore } from "./auth-secret-store.js";
import { ExpoAuthSecretStore } from "../expo/auth-secret-store.js";
import { RecoveryPhrase } from "./recovery-phrase.js";
import { createDb } from "./db.js";

function createMockStorage(): Pick<Storage, "getItem" | "setItem" | "removeItem"> {
  const map = new Map<string, string>();
  return {
    getItem: (k) => map.get(k) ?? null,
    setItem: (k, v) => {
      map.set(k, v);
    },
    removeItem: (k) => {
      map.delete(k);
    },
  };
}

function createMockSecureStore() {
  const map = new Map<string, string>();
  return {
    getItemAsync: (k: string) => Promise.resolve(map.get(k) ?? null),
    setItemAsync: (k: string, v: string) => {
      map.set(k, v);
      return Promise.resolve();
    },
    deleteItemAsync: (k: string) => {
      map.delete(k);
      return Promise.resolve();
    },
  };
}

describe("RecoveryPhrase integration — BrowserAuthSecretStore", () => {
  it("round-trips a secret through backup + restore", async () => {
    const originStorage = createMockStorage();
    const origin = new BrowserAuthSecretStore({ storage: originStorage });
    const original = await origin.getOrCreateSecret();

    const phrase = RecoveryPhrase.fromSecret(original);
    const restored = RecoveryPhrase.toSecret(phrase);
    expect(restored).toBe(original);

    const targetStorage = createMockStorage();
    const target = new BrowserAuthSecretStore({ storage: targetStorage });
    await target.saveSecret(restored);
    expect(await target.loadSecret()).toBe(original);
  });
});

describe("RecoveryPhrase integration — ExpoAuthSecretStore", () => {
  it("round-trips a secret through backup + restore", async () => {
    const originSecureStore = createMockSecureStore();
    const origin = new ExpoAuthSecretStore({ secureStore: originSecureStore });
    const original = await origin.getOrCreateSecret();

    const phrase = RecoveryPhrase.fromSecret(original);
    const restored = RecoveryPhrase.toSecret(phrase);
    expect(restored).toBe(original);

    const targetSecureStore = createMockSecureStore();
    const target = new ExpoAuthSecretStore({ secureStore: targetSecureStore });
    await target.saveSecret(restored);
    expect(await target.loadSecret()).toBe(original);
  });
});

describe("RecoveryPhrase integration — restore over an existing session", () => {
  it("overwrites the browser store's primed cache so the next getOrCreateSecret returns the restored secret", async () => {
    const storage = createMockStorage();
    const store = new BrowserAuthSecretStore({ storage });
    const original = await store.getOrCreateSecret();

    const phrase = RecoveryPhrase.fromSecret(original);
    const replacementBytes = new Uint8Array(32).fill(7);
    let binary = "";
    for (const b of replacementBytes) binary += String.fromCharCode(b);
    const replacement = btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    expect(replacement).not.toBe(original);
    expect(RecoveryPhrase.fromSecret(replacement)).not.toBe(phrase);

    await store.saveSecret(replacement);
    expect(await store.getOrCreateSecret()).toBe(replacement);
  });
});

describe("RecoveryPhrase integration — identity continuity", () => {
  it("two createDb calls with secrets derived from the same phrase yield the same user_id", async () => {
    const originStorage = createMockStorage();
    const origin = new BrowserAuthSecretStore({ storage: originStorage });
    const originalSecret = await origin.getOrCreateSecret();

    const phrase = RecoveryPhrase.fromSecret(originalSecret);
    const restoredSecret = RecoveryPhrase.toSecret(phrase);

    const dbA = await createDb({
      appId: "recovery-phrase-test",
      auth: { localFirstSecret: originalSecret },
    });
    const dbB = await createDb({
      appId: "recovery-phrase-test",
      auth: { localFirstSecret: restoredSecret },
    });

    try {
      const idA = dbA.getAuthState().session?.user_id ?? null;
      const idB = dbB.getAuthState().session?.user_id ?? null;
      expect(idA).not.toBeNull();
      expect(idB).toBe(idA);
    } finally {
      await dbA.shutdown();
      await dbB.shutdown();
    }
  });
});
