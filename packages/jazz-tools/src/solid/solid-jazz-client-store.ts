import { createEffect, onCleanup, type Accessor } from "solid-js";
import { createStore, reconcile } from "solid-js/store";
import type { AuthState } from "../runtime/auth-state.js";
import type { PublicSession } from "../runtime/context.js";
import { withCanonicalUser } from "../runtime/author-id.js";
import type { JazzClient } from "../web/create-jazz-client.js";

interface StoreState {
  authState: AuthState | null;
  session: PublicSession | null;
}

const NULL_STATE: StoreState = {
  authState: null,
  session: null,
};

function getStoreState(client: JazzClient | undefined): StoreState {
  if (!client || typeof client.db?.getAuthState !== "function") {
    return NULL_STATE;
  }

  const authState = client.db.getAuthState();
  return {
    authState,
    session: authState.session ? withCanonicalUser(authState.session) : null,
  };
}

export function createSolidJazzClientStore(client: Accessor<JazzClient | undefined>) {
  const [store, setStore] = createStore<StoreState>(getStoreState(client()));

  createEffect(() => {
    const nextClient = client();

    if (!nextClient || typeof nextClient.db?.onAuthChanged !== "function") {
      setStore(NULL_STATE);
      return;
    }
    setStore(reconcile(getStoreState(nextClient)));

    const unsubscribe = nextClient.db.onAuthChanged((nextAuthState) => {
      setStore(
        reconcile({
          authState: nextAuthState,
          session: nextAuthState.session ? withCanonicalUser(nextAuthState.session) : null,
        }),
      );
    });
    onCleanup(unsubscribe);
  });

  return store;
}
