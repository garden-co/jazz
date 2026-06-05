import { Accessor, createEffect, createSignal, onCleanup } from "solid-js";
import { browserAuthSecretStore, type AuthSecretStore } from "../runtime/auth-secret-store.js";

type ListenerFn = () => void | Promise<void>;

const listenersByStore = new WeakMap<AuthSecretStore, Set<ListenerFn>>();

function getListeners(store: AuthSecretStore): Set<ListenerFn> {
  let listeners = listenersByStore.get(store);
  if (!listeners) {
    listeners = new Set();
    listenersByStore.set(store, listeners);
  }
  return listeners;
}

export type UseLocalFirstAuth = ReturnType<typeof useLocalFirstAuth>;

export function useLocalFirstAuth(store: Accessor<AuthSecretStore> = () => browserAuthSecretStore) {
  const [secret, setSecret] = createSignal<string | null>(null);
  const [isLoading, setIsLoading] = createSignal(true);
  const [error, setError] = createSignal<Error | null>(null);

  createEffect(() => {
    let disposed = false;
    onCleanup(() => {
      disposed = true;
    });

    let latestCallId = 0;

    const refetch = async () => {
      const callId = ++latestCallId;
      const stale = () => disposed || callId !== latestCallId;

      setIsLoading(true);

      try {
        const resolved = await store().getOrCreateSecret();
        if (stale()) {
          return;
        }
        setSecret(resolved);
        setIsLoading(false);
        setError(null);
      } catch (err) {
        if (stale()) {
          return;
        }
        setSecret(null);
        setIsLoading(false);
        setError(normalizeError(err));
      }
    };

    const listeners = getListeners(store());

    refetch();
    listeners.add(refetch);

    onCleanup(() => {
      listeners.delete(refetch);
    });
  });

  return {
    get secret() {
      return secret();
    },
    get isLoading() {
      return isLoading();
    },
    get error() {
      return error();
    },
    async login(nextSecret: string) {
      const currentStore = store();
      const listeners = getListeners(currentStore);
      try {
        await currentStore.saveSecret(nextSecret);
      } finally {
        for (const fn of listeners) fn();
      }
    },
    async signOut() {
      const currentStore = store();
      const listeners = getListeners(currentStore);
      try {
        await currentStore.clearSecret();
      } finally {
        for (const fn of listeners) fn();
      }
    },
  };
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
