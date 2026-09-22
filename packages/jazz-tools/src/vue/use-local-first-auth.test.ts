// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope } from "vue";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

import { useLocalFirstAuth } from "./use-local-first-auth.js";

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

async function waitUntil(check: () => void) {
  return vi.waitFor(check);
}

describe("vue/useLocalFirstAuth", () => {
  let store: AuthSecretStore;

  beforeEach(() => {
    store = makeInMemoryStore();
  });

  it("starts loading=true secret=null, then resolves with a secret", async () => {
    const scope = effectScope();
    const auth = scope.run(() => useLocalFirstAuth(store))!;

    expect(auth.isLoading.value).toBe(true);
    expect(auth.secret.value).toBeNull();

    await waitUntil(() => expect(auth.isLoading.value).toBe(false));
    expect(typeof auth.secret.value).toBe("string");

    scope.stop();
  });

  it("login writes the store and updates secret", async () => {
    const scope = effectScope();
    const auth = scope.run(() => useLocalFirstAuth(store))!;
    await waitUntil(() => expect(auth.isLoading.value).toBe(false));

    await auth.login("provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    await waitUntil(() =>
      expect(auth.secret.value).toBe("provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    );
    expect(auth.isLoading.value).toBe(false);

    scope.stop();
  });

  it("signOut clears the store and rotates the secret on re-resolve", async () => {
    const scope = effectScope();
    const auth = scope.run(() => useLocalFirstAuth(store))!;
    await waitUntil(() => expect(auth.isLoading.value).toBe(false));

    const before = auth.secret.value;
    await auth.signOut();
    await waitUntil(() => {
      expect(auth.secret.value).not.toBeNull();
      expect(auth.secret.value).not.toBe(before);
    });

    scope.stop();
  });

  it("login from one consumer updates another consumer backed by the same store", async () => {
    const aliceScope = effectScope();
    const bobScope = effectScope();
    const alice = aliceScope.run(() => useLocalFirstAuth(store))!;
    const bob = bobScope.run(() => useLocalFirstAuth(store))!;
    await waitUntil(() => {
      expect(alice.isLoading.value).toBe(false);
      expect(bob.isLoading.value).toBe(false);
    });

    await alice.login("shared-secret-aaaaaaaaaaaaaaaaaaaaaaaa");
    await waitUntil(() => {
      expect(alice.secret.value).toBe("shared-secret-aaaaaaaaaaaaaaaaaaaaaaaa");
      expect(bob.secret.value).toBe("shared-secret-aaaaaaaaaaaaaaaaaaaaaaaa");
    });

    aliceScope.stop();
    bobScope.stop();
  });

  it("consumers backed by different stores do not cross-notify", async () => {
    const storeA = makeInMemoryStore();
    const storeB = makeInMemoryStore();
    const aScope = effectScope();
    const bScope = effectScope();
    const authA = aScope.run(() => useLocalFirstAuth(storeA))!;
    const authB = bScope.run(() => useLocalFirstAuth(storeB))!;
    await waitUntil(() => {
      expect(authA.isLoading.value).toBe(false);
      expect(authB.isLoading.value).toBe(false);
    });

    await authA.login("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa");
    await waitUntil(() => expect(authA.secret.value).toBe("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    expect(authB.secret.value).not.toBe("secret-A-aaaaaaaaaaaaaaaaaaaaaaaaaaa");

    aScope.stop();
    bScope.stop();
  });

  it("scope.stop unsubscribes — later notifier bumps do not touch the consumer", async () => {
    const scope = effectScope();
    const auth = scope.run(() => useLocalFirstAuth(store))!;
    await waitUntil(() => expect(auth.isLoading.value).toBe(false));

    const before = auth.secret.value;
    scope.stop();

    const otherScope = effectScope();
    const other = otherScope.run(() => useLocalFirstAuth(store))!;
    await waitUntil(() => expect(other.isLoading.value).toBe(false));

    await other.login("post-cleanup-secret-bbbbbbbbbbbbbbbbbbbbbb");
    await waitUntil(() =>
      expect(other.secret.value).toBe("post-cleanup-secret-bbbbbbbbbbbbbbbbbbbbbb"),
    );
    expect(auth.secret.value).toBe(before);

    otherScope.stop();
  });

  it("an in-flight refetch is discarded if a later refetch overtakes it", async () => {
    const releases: Array<(secret: string) => void> = [];
    const slowStore: AuthSecretStore = {
      loadSecret: async () => null,
      saveSecret: async () => {},
      clearSecret: async () => {},
      getOrCreateSecret() {
        return new Promise<string>((r) => releases.push(r));
      },
    };

    const scope = effectScope();
    const auth = scope.run(() => useLocalFirstAuth(slowStore))!;

    await waitUntil(() => expect(releases.length).toBe(1));

    void auth.login("login-bump");
    await waitUntil(() => expect(releases.length).toBe(2));

    releases[1]("newer-secret-cccccccccccccccccccccccccc");
    await waitUntil(() =>
      expect(auth.secret.value).toBe("newer-secret-cccccccccccccccccccccccccc"),
    );

    releases[0]("older-secret-dddddddddddddddddddddddddd");
    await Promise.resolve();
    expect(auth.secret.value).toBe("newer-secret-cccccccccccccccccccccccccc");

    scope.stop();
  });

  it("rejected login still notifies siblings — partial-commit saves reconverge on store truth", async () => {
    let underlying: string | null = "initial-secret-aaaaaaaaaaaaaaaaaaaaaaaa";
    const partialCommitStore: AuthSecretStore = {
      async loadSecret() {
        return underlying;
      },
      async saveSecret(secret) {
        underlying = secret;
        throw new Error("save partially failed");
      },
      async clearSecret() {
        underlying = null;
      },
      getOrCreateSecret() {
        if (underlying) return Promise.resolve(underlying);
        underlying = "generated";
        return Promise.resolve(underlying);
      },
    };

    const aliceScope = effectScope();
    const bobScope = effectScope();
    const alice = aliceScope.run(() => useLocalFirstAuth(partialCommitStore))!;
    const bob = bobScope.run(() => useLocalFirstAuth(partialCommitStore))!;
    await waitUntil(() => {
      expect(alice.isLoading.value).toBe(false);
      expect(bob.isLoading.value).toBe(false);
    });

    await expect(alice.login("new-secret-eeeeeeeeeeeeeeeeeeeeeeee")).rejects.toThrow(
      "save partially failed",
    );

    await waitUntil(() => {
      expect(alice.secret.value).toBe("new-secret-eeeeeeeeeeeeeeeeeeeeeeee");
      expect(bob.secret.value).toBe("new-secret-eeeeeeeeeeeeeeeeeeeeeeee");
    });

    aliceScope.stop();
    bobScope.stop();
  });

  it("rejected signOut still notifies siblings — partial-commit clears reconverge on store truth", async () => {
    let underlying: string | null = "initial-secret-ffffffffffffffffffffffff";
    const partialCommitStore: AuthSecretStore = {
      async loadSecret() {
        return underlying;
      },
      async saveSecret(secret) {
        underlying = secret;
      },
      async clearSecret() {
        underlying = null;
        throw new Error("clear partially failed");
      },
      getOrCreateSecret() {
        if (underlying) return Promise.resolve(underlying);
        underlying = "generated-gggggggggggggggggggggggggggg";
        return Promise.resolve(underlying);
      },
    };

    const aliceScope = effectScope();
    const bobScope = effectScope();
    const alice = aliceScope.run(() => useLocalFirstAuth(partialCommitStore))!;
    const bob = bobScope.run(() => useLocalFirstAuth(partialCommitStore))!;
    await waitUntil(() => {
      expect(alice.isLoading.value).toBe(false);
      expect(bob.isLoading.value).toBe(false);
    });

    await expect(alice.signOut()).rejects.toThrow("clear partially failed");

    await waitUntil(() => {
      expect(alice.secret.value).toBe("generated-gggggggggggggggggggggggggggg");
      expect(bob.secret.value).toBe("generated-gggggggggggggggggggggggggggg");
    });

    aliceScope.stop();
    bobScope.stop();
  });

  it("warns when called outside an active effect scope (listener would leak)", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const auth = useLocalFirstAuth(store);
    await waitUntil(() => expect(auth.isLoading.value).toBe(false));

    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("[useLocalFirstAuth] called outside an active effect scope"),
    );

    warn.mockRestore();
  });

  it("rejected getOrCreateSecret resolves to secret=null with isLoading=false and warns", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const failingStore: AuthSecretStore = {
      loadSecret: async () => null,
      saveSecret: async () => {},
      clearSecret: async () => {},
      getOrCreateSecret: async () => {
        throw new Error("store unavailable");
      },
    };

    const scope = effectScope();
    const auth = scope.run(() => useLocalFirstAuth(failingStore))!;

    await waitUntil(() => {
      expect(auth.isLoading.value).toBe(false);
      expect(auth.secret.value).toBeNull();
    });
    expect(warn).toHaveBeenCalledWith(
      "[useLocalFirstAuth] secret store failed:",
      expect.any(Error),
    );

    scope.stop();
    warn.mockRestore();
  });
});
