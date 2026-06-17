import { createRoot, createSignal } from "solid-js";
import { beforeEach, describe, expect, it, vi } from "vitest";
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

describe("solid/useLocalFirstAuth", () => {
  let store: AuthSecretStore;

  beforeEach(() => {
    store = makeInMemoryStore();
  });

  it("SD-LFA-01: loads initial secret and clears loading state", async () => {
    let dispose!: () => void;
    let auth!: ReturnType<typeof useLocalFirstAuth>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        auth = useLocalFirstAuth(() => store);
        return undefined;
      });

      expect(auth.isLoading).toBe(true);
      expect(auth.secret).toBeNull();

      await vi.waitFor(() => expect(auth.isLoading).toBe(false));
      expect(typeof auth.secret).toBe("string");
      expect(auth.error).toBeNull();
    } finally {
      dispose?.();
    }
  });

  it("SD-LFA-02: login persists secret and updates local state", async () => {
    const PROVIDED_SECRET = "provided-secret-aaaaaaaaaaaaaaaaaaaaaaaaaaaa" as const;

    let dispose!: () => void;
    let auth!: ReturnType<typeof useLocalFirstAuth>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        auth = useLocalFirstAuth(() => store);
        return undefined;
      });

      await vi.waitFor(() => expect(auth.isLoading).toBe(false));

      await auth.login(PROVIDED_SECRET);

      await vi.waitFor(() => {
        expect(auth.isLoading).toBe(false);
        expect(auth.secret).toBe(PROVIDED_SECRET);
      });
    } finally {
      dispose?.();
    }
  });

  it("SD-LFA-03: signOut clears persisted secret and resolves a new one", async () => {
    let dispose!: () => void;
    let auth!: ReturnType<typeof useLocalFirstAuth>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        auth = useLocalFirstAuth(() => store);
        return undefined;
      });

      await vi.waitFor(() => expect(auth.isLoading).toBe(false));
      const before = auth.secret;

      await auth.signOut();

      await vi.waitFor(() => {
        expect(auth.isLoading).toBe(false);
        expect(auth.secret).not.toBeNull();
        expect(auth.secret).not.toBe(before);
      });
    } finally {
      dispose?.();
    }
  });

  it("SD-LFA-04: notifies other consumers that share the same store", async () => {
    const SHARED_SECRET = "shared-secret-bbbbbbbbbbbbbbbbbbbbbbbbbbbb" as const;

    let disposeA!: () => void;
    let disposeB!: () => void;
    let alice!: ReturnType<typeof useLocalFirstAuth>;
    let bob!: ReturnType<typeof useLocalFirstAuth>;

    try {
      createRoot((rootDispose) => {
        disposeA = rootDispose;
        alice = useLocalFirstAuth(() => store);
        return undefined;
      });

      createRoot((rootDispose) => {
        disposeB = rootDispose;
        bob = useLocalFirstAuth(() => store);
        return undefined;
      });

      await vi.waitFor(() => {
        expect(alice.isLoading).toBe(false);
        expect(bob.isLoading).toBe(false);
      });

      await alice.login(SHARED_SECRET);

      await vi.waitFor(() => {
        expect(alice.secret).toBe(SHARED_SECRET);
        expect(bob.secret).toBe(SHARED_SECRET);
      });
    } finally {
      disposeA?.();
      disposeB?.();
    }
  });

  it("SD-LFA-05: does not cross-notify consumers backed by different stores", async () => {
    const STORE_A_SECRET = "secret-A-cccccccccccccccccccccccccccc" as const;

    const storeA = makeInMemoryStore();
    const storeB = makeInMemoryStore();

    let disposeA!: () => void;
    let disposeB!: () => void;
    let authA!: ReturnType<typeof useLocalFirstAuth>;
    let authB!: ReturnType<typeof useLocalFirstAuth>;

    try {
      createRoot((rootDispose) => {
        disposeA = rootDispose;
        authA = useLocalFirstAuth(() => storeA);
        return undefined;
      });

      createRoot((rootDispose) => {
        disposeB = rootDispose;
        authB = useLocalFirstAuth(() => storeB);
        return undefined;
      });

      await vi.waitFor(() => {
        expect(authA.isLoading).toBe(false);
        expect(authB.isLoading).toBe(false);
      });

      await authA.login(STORE_A_SECRET);

      await vi.waitFor(() => expect(authA.secret).toBe(STORE_A_SECRET));
      expect(authB.secret).not.toBe(STORE_A_SECRET);
    } finally {
      disposeA?.();
      disposeB?.();
    }
  });

  it("SD-LFA-06: ignores stale in-flight secret fetch after store switch", async () => {
    const NEW_STORE_SECRET = "new-store-secret-dddddddddddddddddddddddddd" as const;
    const OLD_STORE_SECRET = "old-store-secret-eeeeeeeeeeeeeeeeeeeeeeee" as const;

    const releasesA: Array<(value: string) => void> = [];
    const releasesB: Array<(value: string) => void> = [];

    const storeA: AuthSecretStore = {
      loadSecret: async () => null,
      saveSecret: async () => {},
      clearSecret: async () => {},
      getOrCreateSecret() {
        return new Promise<string>((resolve) => releasesA.push(resolve));
      },
    };

    const storeB: AuthSecretStore = {
      loadSecret: async () => null,
      saveSecret: async () => {},
      clearSecret: async () => {},
      getOrCreateSecret() {
        return new Promise<string>((resolve) => releasesB.push(resolve));
      },
    };

    let dispose!: () => void;
    let auth!: ReturnType<typeof useLocalFirstAuth>;
    let setActiveStore!: (next: AuthSecretStore) => void;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        const [activeStore, _setActiveStore] = createSignal<AuthSecretStore>(storeA);
        setActiveStore = _setActiveStore;
        auth = useLocalFirstAuth(activeStore);
        return undefined;
      });

      await vi.waitFor(() => expect(releasesA.length).toBe(1));

      setActiveStore(storeB);

      await vi.waitFor(() => expect(releasesB.length).toBe(1));

      releasesB[0](NEW_STORE_SECRET);
      await vi.waitFor(() => expect(auth.secret).toBe(NEW_STORE_SECRET));

      releasesA[0](OLD_STORE_SECRET);
      await Promise.resolve();

      expect(auth.secret).toBe(NEW_STORE_SECRET);
    } finally {
      dispose?.();
    }
  });
});
