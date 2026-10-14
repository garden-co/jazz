import type { AuthSecretStore } from "./auth-secret-store.js";
import { toError } from "./util.js";

export interface LocalFirstAuthState {
  secret: string | null;
  isLoading: boolean;
  /** Non-null when loading the durable root failed (for example, malformed storage). */
  error: Error | null;
}

/**
 * Framework-neutral local-first auth state for one {@link AuthSecretStore}.
 * Framework adapters mirror {@link getState} into their own reactivity.
 */
export interface LocalFirstAuthController {
  getState(): LocalFirstAuthState;
  /** The first subscriber triggers a load; the last one leaving discards the state. */
  subscribe(listener: () => void): () => void;
  /**
   * Subscribe and apply the current state immediately and on every change.
   * With `warnLabel`, each new load failure is also logged under that label.
   */
  watch(apply: (state: LocalFirstAuthState) => void, warnLabel?: string): () => void;
  /** Save `secret`, then reload every subscriber (even if the save rejected). */
  login(secret: string): Promise<void>;
  /** Clear the stored secret, then reload every subscriber (even if the clear rejected). */
  signOut(): Promise<void>;
}

export const initialLocalFirstAuthState: LocalFirstAuthState = {
  secret: null,
  isLoading: true,
  error: null,
};

const controllersByStore = new WeakMap<AuthSecretStore, LocalFirstAuthController>();

/** Shared controller for `store`, so every consumer of a store sees login and sign-out. */
export function getLocalFirstAuthController(store: AuthSecretStore): LocalFirstAuthController {
  let controller = controllersByStore.get(store);
  if (!controller) {
    controller = createLocalFirstAuthController(store);
    controllersByStore.set(store, controller);
  }
  return controller;
}

function createLocalFirstAuthController(store: AuthSecretStore): LocalFirstAuthController {
  const listeners = new Set<() => void>();
  let state = initialLocalFirstAuthState;
  let latestCallId = 0;

  const setState = (next: LocalFirstAuthState) => {
    state = next;
    for (const listener of listeners) listener();
  };

  const reload = () => {
    const callId = ++latestCallId;
    const onError = (error: unknown) => {
      if (callId !== latestCallId) return;
      setState({
        secret: null,
        isLoading: false,
        error: toError(error),
      });
    };

    if (!state.isLoading) setState({ ...state, isLoading: true });
    try {
      store.getOrCreateSecret().then((secret) => {
        if (callId !== latestCallId) return;
        setState({ secret, isLoading: false, error: null });
      }, onError);
    } catch (error) {
      onError(error);
    }
  };

  const reloadSubscribers = () => {
    if (listeners.size > 0) reload();
  };

  const subscribe = (listener: () => void) => {
    listeners.add(listener);
    if (listeners.size === 1) reload();
    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) {
        latestCallId += 1;
        state = initialLocalFirstAuthState;
      }
    };
  };

  return {
    getState: () => state,
    subscribe,
    watch(apply, warnLabel) {
      let warnedError: Error | null = null;
      const sync = () => {
        const current = state;
        if (warnLabel && current.error && current.error !== warnedError) {
          warnedError = current.error;
          console.warn(`[${warnLabel}] secret store failed:`, current.error);
        }
        apply(current);
      };
      const unsubscribe = subscribe(sync);
      sync();
      return unsubscribe;
    },
    async login(secret) {
      try {
        await store.saveSecret(secret);
      } finally {
        reloadSubscribers();
      }
    },
    async signOut() {
      try {
        await store.clearSecret();
      } finally {
        reloadSubscribers();
      }
    },
  };
}
