import { Accessor, createEffect, createSignal, onCleanup } from "solid-js";
import { browserAuthSecretStore, type AuthSecretStore } from "../runtime/auth-secret-store.js";
import {
  getLocalFirstAuthController,
  initialLocalFirstAuthState,
} from "../runtime/local-first-auth-controller.js";

export type UseLocalFirstAuth = ReturnType<typeof useLocalFirstAuth>;

export function useLocalFirstAuth(store: Accessor<AuthSecretStore> = () => browserAuthSecretStore) {
  const [state, setState] = createSignal(initialLocalFirstAuthState);

  createEffect(() => {
    onCleanup(getLocalFirstAuthController(store()).watch(setState));
  });

  return {
    get secret() {
      return state().secret;
    },
    get isLoading() {
      return state().isLoading;
    },
    get error() {
      return state().error;
    },
    login(nextSecret: string) {
      return getLocalFirstAuthController(store()).login(nextSecret);
    },
    signOut() {
      return getLocalFirstAuthController(store()).signOut();
    },
  };
}
