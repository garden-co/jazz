import { useSyncExternalStore } from "react";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";
import {
  getLocalFirstAuthController,
  initialLocalFirstAuthState,
} from "../runtime/local-first-auth-controller.js";

export interface LocalFirstAuth {
  secret: string | null;
  isLoading: boolean;
  /** Non-null when loading the durable root failed (for example, malformed storage). */
  error: Error | null;
  login(secret: string): Promise<void>;
  signOut(): Promise<void>;
}

export function createUseLocalFirstAuth(store: AuthSecretStore): () => LocalFirstAuth {
  return function useLocalFirstAuth(): LocalFirstAuth {
    return useLocalFirstAuthWithStore(store);
  };
}

const getServerState = () => initialLocalFirstAuthState;

export function useLocalFirstAuthWithStore(store: AuthSecretStore): LocalFirstAuth {
  const controller = getLocalFirstAuthController(store);
  const state = useSyncExternalStore(controller.subscribe, controller.getState, getServerState);
  return { ...state, login: controller.login, signOut: controller.signOut };
}
