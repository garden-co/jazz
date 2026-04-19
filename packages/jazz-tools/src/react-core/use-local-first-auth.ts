import { useEffect, useState, useSyncExternalStore } from "react";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

export interface LocalFirstAuth {
  secret: string | null;
  isLoading: boolean;
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

  const subscribe = (onChange: () => void): (() => void) => {
    listeners.add(onChange);
    return () => {
      listeners.delete(onChange);
    };
  };

  const getSnapshot = (): number => version;
  const getServerSnapshot = (): number => 0;

  async function login(secret: string): Promise<void> {
    await store.saveSecret(secret);
    notify();
  }

  async function signOut(): Promise<void> {
    await store.clearSecret();
    notify();
  }

  return function useLocalFirstAuth(): LocalFirstAuth {
    const currentVersion = useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
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
    }, [currentVersion]);

    return { secret, isLoading, login, signOut };
  };
}
