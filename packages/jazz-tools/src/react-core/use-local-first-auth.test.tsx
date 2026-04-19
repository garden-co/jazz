import { describe, it, expect, beforeEach } from "vitest";
import { renderHook, act, waitFor } from "@testing-library/react";
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

  it("starts with isLoading=true and secret=null, then resolves", async () => {
    const { result } = renderHook(() => useLocalFirstAuth());
    expect(result.current.isLoading).toBe(true);
    expect(result.current.secret).toBeNull();

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(typeof result.current.secret).toBe("string");
  });

  it("login writes the store and updates secret", async () => {
    const { result } = renderHook(() => useLocalFirstAuth());
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    await act(async () => {
      await result.current.login("provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    });

    await waitFor(() =>
      expect(result.current.secret).toBe("provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    );
  });

  it("signOut clears the store and rotates the secret on re-resolve", async () => {
    const { result } = renderHook(() => useLocalFirstAuth());
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const before = result.current.secret;

    await act(async () => {
      await result.current.signOut();
    });

    await waitFor(() => {
      expect(result.current.secret).not.toBeNull();
      expect(result.current.secret).not.toBe(before);
    });
  });

  it("two factory invocations have independent version scopes", async () => {
    const storeA = makeInMemoryStore();
    const storeB = makeInMemoryStore();
    const useA = createUseLocalFirstAuth(storeA);
    const useB = createUseLocalFirstAuth(storeB);
    const { result: a } = renderHook(() => useA());
    const { result: b } = renderHook(() => useB());

    await waitFor(() => expect(a.current.isLoading).toBe(false));
    await waitFor(() => expect(b.current.isLoading).toBe(false));

    await act(async () => {
      await a.current.login("secret-A");
    });

    await waitFor(() => expect(a.current.secret).toBe("secret-A"));
    expect(b.current.secret).not.toBe("secret-A");
  });
});
