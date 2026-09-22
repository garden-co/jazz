import { createEffect, onCleanup, type Accessor } from "solid-js";
import { createStore, reconcile } from "solid-js/store";
import type { AuthState } from "../runtime/auth-state.js";
import type { PublicSession } from "../runtime/context.js";
import type { JazzClient } from "../web/create-jazz-client.js";

interface StoreState {
  authState: AuthState | null;
  session: PublicSession | null;
}

function emptyState(): StoreState {
  // Solid stores mutate their backing object. Never share the empty object
  // between providers, or one account's session can overwrite another's.
  return { authState: null, session: null };
}

function getStoreState(client: JazzClient | undefined): StoreState {
  if (!client || typeof client.db?.getAuthState !== "function") {
    return emptyState();
  }

  const authState = client.db.getAuthState();
  return {
    authState,
    session: authState.session,
  };
}

export function createSolidJazzClientStore(client: Accessor<JazzClient | undefined>) {
  const [store, setStore] = createStore<StoreState>(getStoreState(client()));

  createEffect(() => {
    const nextClient = client();

    if (!nextClient || typeof nextClient.db?.onAuthChanged !== "function") {
      setStore(emptyState());
      return;
    }
    setStore(reconcile(getStoreState(nextClient)));

    const unsubscribe = nextClient.db.onAuthChanged((nextAuthState) => {
      setStore(
        reconcile({
          authState: nextAuthState,
          session: nextAuthState.session,
        }),
      );
    });
    onCleanup(unsubscribe);
  });

  return store;
}
