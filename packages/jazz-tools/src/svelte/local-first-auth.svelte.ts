import { browserAuthSecretStore, type AuthSecretStore } from "../runtime/auth-secret-store.js";

const listenersByStore = new WeakMap<AuthSecretStore, Set<() => void>>();

function getListeners(store: AuthSecretStore): Set<() => void> {
  let listeners = listenersByStore.get(store);
  if (!listeners) {
    listeners = new Set();
    listenersByStore.set(store, listeners);
  }
  return listeners;
}

/**
 * Reactive local-first auth secret. Mirrors `useLocalFirstAuth()` from
 * `jazz-tools/react`. Instantiate in a Svelte component `<script>` block.
 *
 * The secret store is only read inside `$effect`, which the Svelte compiler
 * skips on the server, so SvelteKit server renders see `secret: null,
 * isLoading: true` and never touch `localStorage`.
 *
 * `login` and `signOut` update every live instance backed by the same store,
 * so a sign-out button anywhere in the tree invalidates a provider mounted
 * higher up without a manual reload.
 *
 * ```svelte
 * <script lang="ts">
 *   import { LocalFirstAuth, createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
 *   import TodoList from './TodoList.svelte';
 *
 *   const auth = new LocalFirstAuth();
 *
 *   let client = $derived(
 *     !auth.isLoading && auth.secret
 *       ? createJazzClient({ appId: '<your-app-id>', secret: auth.secret })
 *       : null,
 *   );
 * </script>
 *
 * {#if client}
 *   <JazzSvelteProvider {client}>
 *     {#snippet children()}
 *       <TodoList />
 *     {/snippet}
 *   </JazzSvelteProvider>
 * {/if}
 * ```
 *
 * @param store - optional {@link AuthSecretStore} override. Defaults to the
 *   shared {@link browserAuthSecretStore} singleton. Pass a custom store to
 *   isolate secrets per app, user, or session, or to swap in an alternative
 *   storage backend.
 */
export class LocalFirstAuth {
  secret: string | null = $state(null);
  isLoading: boolean = $state(true);

  login: (secret: string) => Promise<void>;
  signOut: () => Promise<void>;

  constructor(store: AuthSecretStore = browserAuthSecretStore) {
    const listeners = getListeners(store);

    const notify = () => {
      for (const fn of listeners) fn();
    };

    this.login = async (secret: string) => {
      try {
        await store.saveSecret(secret);
      } finally {
        notify();
      }
    };

    this.signOut = async () => {
      try {
        await store.clearSecret();
      } finally {
        notify();
      }
    };

    $effect(() => {
      let cancelled = false;
      let latestCallId = 0;

      const refetch = () => {
        const callId = ++latestCallId;
        const stale = () => cancelled || callId !== latestCallId;
        const onError = (err: unknown) => {
          if (stale()) return;
          console.warn("[LocalFirstAuth] secret store failed:", err);
          this.secret = null;
          this.isLoading = false;
        };

        this.isLoading = true;
        try {
          store
            .getOrCreateSecret()
            .then((resolved) => {
              if (stale()) return;
              this.secret = resolved;
              this.isLoading = false;
            })
            .catch(onError);
        } catch (err) {
          onError(err);
        }
      };

      refetch();
      listeners.add(refetch);

      return () => {
        cancelled = true;
        listeners.delete(refetch);
      };
    });
  }
}
