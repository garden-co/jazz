import { describe, it, expect, beforeEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { createUseLocalFirstAuth } from "./use-local-first-auth.js";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

function makeInMemoryStore(): AuthSecretStore {
  let value: string | null = null;
  let cached: Promise<string> | null = null;
  return {
    async loadSecret() {
      return value;
    },
    async saveSecret(secret) {
      value = secret;
      cached = Promise.resolve(secret);
    },
    async clearSecret() {
      value = null;
      cached = null;
    },
    getOrCreateSecret() {
      if (!cached) {
        value = value ?? `generated-${Math.random().toString(36).slice(2)}`;
        cached = Promise.resolve(value);
      }
      return cached;
    },
  };
}

describe("createUseLocalFirstAuth", () => {
  let store: AuthSecretStore;
  let useLocalFirstAuth: ReturnType<typeof createUseLocalFirstAuth>;

  beforeEach(() => {
    store = makeInMemoryStore();
    useLocalFirstAuth = createUseLocalFirstAuth(store);
  });

  it("getOrCreateSecret caches and returns stable promise identity", async () => {
    const { result } = renderHook(() => useLocalFirstAuth());
    const p1 = result.current.getOrCreateSecret();
    const p2 = result.current.getOrCreateSecret();
    expect(p1).toBe(p2);
    const s = await p1;
    expect(typeof s).toBe("string");
  });

  it("login writes the store and bumps version", async () => {
    const { result } = renderHook(() => useLocalFirstAuth());
    await act(async () => {
      await result.current.login("provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    });
    const s = await result.current.getOrCreateSecret();
    expect(s).toBe("provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  });

  it("signOut clears the store; next getOrCreateSecret rotates identity", async () => {
    const { result } = renderHook(() => useLocalFirstAuth());
    const before = await result.current.getOrCreateSecret();
    await act(async () => {
      await result.current.signOut();
    });
    const after = await result.current.getOrCreateSecret();
    expect(after).not.toBe(before);
  });

  it("two factory invocations have independent version scopes", async () => {
    const storeA = makeInMemoryStore();
    const storeB = makeInMemoryStore();
    const useA = createUseLocalFirstAuth(storeA);
    const useB = createUseLocalFirstAuth(storeB);
    const { result: a } = renderHook(() => useA());
    const { result: b } = renderHook(() => useB());
    await act(async () => {
      await a.current.login("secret-A");
    });
    expect(await a.current.getOrCreateSecret()).toBe("secret-A");
    expect(await b.current.getOrCreateSecret()).not.toBe("secret-A");
  });
});
