import { useEffect, useState, useSyncExternalStore } from "react";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

export interface LocalFirstAuth {
  secret: string | null;
  isLoading: boolean;
  login(secret: string): Promise<void>;
  signOut(): Promise<void>;
}

interface LocalFirstAuthStoreState {
  subscribe(onChange: () => void): () => void;
  getSnapshot(): number;
  login(secret: string): Promise<void>;
  signOut(): Promise<void>;
}

const storeStates = new WeakMap<AuthSecretStore, LocalFirstAuthStoreState>();
const getServerSnapshot = (): number => 0;

function getStoreState(store: AuthSecretStore): LocalFirstAuthStoreState {
  const existing = storeStates.get(store);
  if (existing) {
    return existing;
  }

  let version = 0;
  const listeners = new Set<() => void>();

  const notify = () => {
    version += 1;
    for (const l of listeners) l();
  };

  const subscribe = (onChange: () => void): (() => void) => {
    listeners.add(onChange);
    return () => {
      listeners.delete(onChange);
    };
  };

  const getSnapshot = (): number => version;

  async function login(secret: string): Promise<void> {
    await store.saveSecret(secret);
    notify();
  }

  async function signOut(): Promise<void> {
    await store.clearSecret();
    notify();
  }

  const state = { subscribe, getSnapshot, login, signOut };
  storeStates.set(store, state);
  return state;
}

export function useLocalFirstAuthWithStore(store: AuthSecretStore): LocalFirstAuth {
  const storeState = getStoreState(store);
  const currentVersion = useSyncExternalStore(
    storeState.subscribe,
    storeState.getSnapshot,
    getServerSnapshot,
  );
  const [secret, setSecret] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    store
      .getOrCreateSecret()
      .then((resolved) => {
        if (cancelled) return;
        setSecret(resolved);
        setIsLoading(false);
      })
      .catch(() => {
        if (cancelled) return;
        setSecret(null);
        setIsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [currentVersion, store]);

  return { secret, isLoading, login: storeState.login, signOut: storeState.signOut };
}
