import { useSyncExternalStore } from "react";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

export interface LocalFirstAuth {
  getOrCreateSecret(): Promise<string>;
  login(secret: string): Promise<void>;
  signOut(): Promise<void>;
}

export function createUseLocalFirstAuth(store: AuthSecretStore): () => LocalFirstAuth {
  let version = 0;
  const listeners = new Set<() => void>();

  const notify = () => {
    version += 1;
    for (const l of listeners) l();
  };

  const api: LocalFirstAuth = {
    getOrCreateSecret: () => store.getOrCreateSecret(),
    async login(secret: string): Promise<void> {
      await store.saveSecret(secret);
      notify();
    },
    async signOut(): Promise<void> {
      await store.clearSecret();
      notify();
    },
  };

  const subscribe = (onChange: () => void): (() => void) => {
    listeners.add(onChange);
    return () => {
      listeners.delete(onChange);
    };
  };

  const getSnapshot = (): number => version;

  return function useLocalFirstAuth(): LocalFirstAuth {
    useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
    return api;
  };
}
