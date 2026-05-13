import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushSync } from "svelte";
import "./test-helpers.svelte.js";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

const { LocalFirstAuth } = await import("./local-first-auth.svelte.js");

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

function waitUntil(check: () => void) {
  return vi.waitFor(() => {
    flushSync();
    check();
  });
}

describe("svelte/LocalFirstAuth", () => {
  let store: AuthSecretStore;

  beforeEach(() => {
    store = makeInMemoryStore();
  });

  it("starts loading=true secret=null, then resolves with a secret", async () => {
    let auth!: InstanceType<typeof LocalFirstAuth>;
    const cleanup = $effect.root(() => {
      auth = new LocalFirstAuth(store);
    });

    expect(auth.isLoading).toBe(true);
    expect(auth.secret).toBeNull();

    await waitUntil(() => expect(auth.isLoading).toBe(false));
    expect(typeof auth.secret).toBe("string");

    cleanup();
  });

  it("login writes the store and updates secret", async () => {
    let auth!: InstanceType<typeof LocalFirstAuth>;
    const cleanup = $effect.root(() => {
      auth = new LocalFirstAuth(store);
    });
    await waitUntil(() => expect(auth.isLoading).toBe(false));

    await auth.login("provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    await waitUntil(() => expect(auth.secret).toBe("provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    expect(auth.isLoading).toBe(false);

    cleanup();
  });

  it("signOut clears the store and rotates the secret on re-resolve", async () => {
    let auth!: InstanceType<typeof LocalFirstAuth>;
    const cleanup = $effect.root(() => {
      auth = new LocalFirstAuth(store);
    });
    await waitUntil(() => expect(auth.isLoading).toBe(false));

    const before = auth.secret;

    await auth.signOut();
    await waitUntil(() => {
      expect(auth.secret).not.toBeNull();
      expect(auth.secret).not.toBe(before);
    });

    cleanup();
  });

  it("login from one instance updates a second instance backed by the same store", async () => {
    let aliceAuth!: InstanceType<typeof LocalFirstAuth>;
    let bobAuth!: InstanceType<typeof LocalFirstAuth>;
    const cleanup = $effect.root(() => {
      aliceAuth = new LocalFirstAuth(store);
      bobAuth = new LocalFirstAuth(store);
    });
    await waitUntil(() => expect(bobAuth.isLoading).toBe(false));

    await aliceAuth.login("shared-secret-aaaaaaaaaaaaaaaaaaaaaaaa");
    await waitUntil(() => {
      expect(aliceAuth.secret).toBe("shared-secret-aaaaaaaaaaaaaaaaaaaaaaaa");
      expect(bobAuth.secret).toBe("shared-secret-aaaaaaaaaaaaaaaaaaaaaaaa");
    });

    cleanup();
  });

  it("instances backed by different stores do not cross-notify", async () => {
    const storeA = makeInMemoryStore();
    const storeB = makeInMemoryStore();
    let authA!: InstanceType<typeof LocalFirstAuth>;
    let authB!: InstanceType<typeof LocalFirstAuth>;
    const cleanup = $effect.root(() => {
      authA = new LocalFirstAuth(storeA);
      authB = new LocalFirstAuth(storeB);
    });
    await waitUntil(() => {
      expect(authA.isLoading).toBe(false);
      expect(authB.isLoading).toBe(false);
    });

    await authA.login("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa");
    await waitUntil(() => expect(authA.secret).toBe("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    expect(authB.secret).not.toBe("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa");

    cleanup();
  });

  it("cleanup unsubscribes — later notifier bumps do not touch the instance", async () => {
    let auth!: InstanceType<typeof LocalFirstAuth>;
    const cleanup = $effect.root(() => {
      auth = new LocalFirstAuth(store);
    });
    await waitUntil(() => expect(auth.isLoading).toBe(false));

    const before = auth.secret;
    cleanup();

    let other!: InstanceType<typeof LocalFirstAuth>;
    const cleanup2 = $effect.root(() => {
      other = new LocalFirstAuth(store);
    });
    await waitUntil(() => expect(other.isLoading).toBe(false));

    await other.login("post-cleanup-secret-bbbbbbbbbbbbbbbbbbbbbb");
    await waitUntil(() => expect(other.secret).toBe("post-cleanup-secret-bbbbbbbbbbbbbbbbbbbbbb"));
    expect(auth.secret).toBe(before);

    cleanup2();
  });

  it("isLoading flips back to true while a bump-driven refetch is in flight", async () => {
    let release!: (secret: string) => void;
    let pending: Promise<string> | null = null;
    let resolved: string | null = "initial-secret-aaaaaaaaaaaaaaaaaaaaaaaaa";
    const slowStore: AuthSecretStore = {
      loadSecret: async () => resolved,
      saveSecret: async (s) => {
        resolved = s;
      },
      clearSecret: async () => {
        resolved = null;
      },
      getOrCreateSecret() {
        pending = new Promise<string>((r) => {
          release = (s) => {
            resolved = s;
            r(s);
          };
        });
        return pending;
      },
    };

    let auth!: InstanceType<typeof LocalFirstAuth>;
    const cleanup = $effect.root(() => {
      auth = new LocalFirstAuth(slowStore);
    });

    await waitUntil(() => expect(pending).not.toBeNull());
    expect(auth.isLoading).toBe(true);
    release("initial-secret-aaaaaaaaaaaaaaaaaaaaaaaaa");
    await waitUntil(() => expect(auth.isLoading).toBe(false));

    pending = null;
    void auth.login("new-secret-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    await waitUntil(() => {
      expect(auth.isLoading).toBe(true);
      expect(pending).not.toBeNull();
    });

    release("new-secret-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    await waitUntil(() => {
      expect(auth.isLoading).toBe(false);
      expect(auth.secret).toBe("new-secret-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    });

    cleanup();
  });

  it("rejected getOrCreateSecret resolves to secret=null with isLoading=false", async () => {
    const failingStore: AuthSecretStore = {
      loadSecret: async () => null,
      saveSecret: async () => {},
      clearSecret: async () => {},
      getOrCreateSecret: async () => {
        throw new Error("store unavailable");
      },
    };

    let auth!: InstanceType<typeof LocalFirstAuth>;
    const cleanup = $effect.root(() => {
      auth = new LocalFirstAuth(failingStore);
    });

    await waitUntil(() => {
      expect(auth.isLoading).toBe(false);
      expect(auth.secret).toBeNull();
    });

    cleanup();
  });
});
