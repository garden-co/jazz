import { createRoot, createSignal, type Accessor } from "solid-js";
import { describe, expect, it } from "vitest";
import type { AuthState } from "../runtime/auth-state.js";
import type { JazzClient } from "../web/create-jazz-client.js";
import { createSolidJazzClientStore } from "./solid-jazz-client-store.js";

type Listener = (state: AuthState) => void;

type MockClient = {
  db: {
    getAuthState: () => AuthState;
    onAuthChanged: (cb: Listener) => () => void;
  };
};

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
}

function createMockClient(initial: AuthState) {
  let state = initial;
  const listeners = new Set<Listener>();

  const client: MockClient = {
    db: {
      getAuthState: () => state,
      onAuthChanged: (cb: Listener) => {
        listeners.add(cb);
        return () => {
          listeners.delete(cb);
        };
      },
    },
  };

  return {
    client,
    emit(next: AuthState) {
      state = next;
      for (const listener of listeners) {
        listener(next);
      }
    },
    listenerCount() {
      return listeners.size;
    },
  };
}

function makeStateA(): AuthState {
  return {
    authMode: "external",
    session: {
      user_id: "u-a",
      claims: { role: "reader" },
      authMode: "external",
    },
  };
}

function makeStateB(): AuthState {
  return {
    authMode: "local-first",
    session: {
      user_id: "u-b",
      claims: { role: "writer" },
      authMode: "local-first",
    },
  };
}

describe("solid/createJazzClientStateStore", () => {
  it("SD-CSTATE-01: initializes from current auth state and reacts to auth updates", async () => {
    const a = createMockClient(makeStateA());
    let store!: ReturnType<typeof createSolidJazzClientStore>;
    let dispose!: () => void;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        store = createSolidJazzClientStore((() => a.client) as Accessor<JazzClient | undefined>);

        expect(store.authState?.authMode).toBe("external");
        expect(store.session?.user_id).toBe("u-a");

        a.emit({
          authMode: "external",
          session: {
            user_id: "u-a2",
            claims: { role: "reader" },
            authMode: "external",
          },
        });
        return undefined;
      });

      await flushMicrotasks();
      expect(store.session?.user_id).toBe("u-a2");
    } finally {
      dispose?.();
    }
    expect(a.listenerCount()).toBe(0);
  });

  it("SD-CSTATE-02: switches auth subscription when client accessor changes", async () => {
    const a = createMockClient(makeStateA());
    const b = createMockClient(makeStateB());

    let setClient!: (next: MockClient | undefined) => void;
    let store!: ReturnType<typeof createSolidJazzClientStore>;
    let dispose!: () => void;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        const [client, _setClient] = createSignal<MockClient | undefined>(a.client);
        setClient = _setClient;
        store = createSolidJazzClientStore(client as Accessor<JazzClient | undefined>);
        return undefined;
      });

      await flushMicrotasks();

      expect(store.session?.user_id).toBe("u-a");
      expect(a.listenerCount()).toBe(1);
      expect(b.listenerCount()).toBe(0);

      setClient(b.client);
      await flushMicrotasks();

      expect(store.session?.user_id).toBe("u-b");
      expect(a.listenerCount()).toBe(0);
      expect(b.listenerCount()).toBe(1);

      a.emit({
        authMode: "external",
        session: {
          user_id: "u-a-stale",
          claims: {},
          authMode: "external",
        },
      });
      expect(store.session?.user_id).toBe("u-b");

      b.emit({
        authMode: "local-first",
        session: {
          user_id: "u-b2",
          claims: {},
          authMode: "local-first",
        },
      });
      await flushMicrotasks();

      expect(store.session?.user_id).toBe("u-b2");
    } finally {
      dispose?.();
    }
    expect(a.listenerCount()).toBe(0);
    expect(b.listenerCount()).toBe(0);
  });

  it("SD-CSTATE-03: clears auth/session when client becomes undefined and unsubscribes", async () => {
    const a = createMockClient(makeStateA());

    let setClient!: (next: MockClient | undefined) => void;
    let store!: ReturnType<typeof createSolidJazzClientStore>;
    let dispose!: () => void;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        const [client, _setClient] = createSignal<MockClient | undefined>(a.client);
        setClient = _setClient;
        store = createSolidJazzClientStore(client as Accessor<JazzClient | undefined>);
        return undefined;
      });

      await flushMicrotasks();
      expect(store.session?.user_id).toBe("u-a");
      expect(a.listenerCount()).toBe(1);

      setClient(undefined);
      await flushMicrotasks();

      expect(store.authState).toBeNull();
      expect(store.session).toBeNull();
      expect(a.listenerCount()).toBe(0);
    } finally {
      dispose?.();
    }
    expect(a.listenerCount()).toBe(0);
  });
});
