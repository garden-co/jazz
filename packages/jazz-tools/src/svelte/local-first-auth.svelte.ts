import { browserAuthSecretStore, type AuthSecretStore } from "../runtime/auth-secret-store.js";
import { getLocalFirstAuthController } from "../runtime/local-first-auth-controller.js";

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
 *   import { LocalFirstAuth, JazzSvelteProvider } from 'jazz-tools/svelte';
 *   import TodoList from './TodoList.svelte';
 *
 *   const auth = new LocalFirstAuth();
 *
 *   let config = $derived(
 *     !auth.isLoading && auth.secret
 *       ? { appId: '<your-app-id>', secret: auth.secret }
 *       : null,
 *   );
 * </script>
 *
 * {#if config}
 *   <JazzSvelteProvider {config}>
 *     {#snippet children()}
 *       <TodoList />
 *     {/snippet}
 *   </JazzSvelteProvider>
 * {/if}
 * ```
 *
 * @param store - optional {@link AuthSecretStore} override. Defaults to the
 *   shared {@link browserAuthSecretStore} singleton. Pass a custom store to
 *   select an explicit local app/profile scope or an alternative storage
 *   backend. Do not scope a durable local-first root by external user or
 *   transient session identity.
 */
export class LocalFirstAuth {
  secret: string | null = $state(null);
  isLoading: boolean = $state(true);

  login: (secret: string) => Promise<void>;
  signOut: () => Promise<void>;

  constructor(store: AuthSecretStore = browserAuthSecretStore) {
    const controller = getLocalFirstAuthController(store);
    this.login = controller.login;
    this.signOut = controller.signOut;

    $effect(() =>
      controller.watch((state) => {
        this.secret = state.secret;
        this.isLoading = state.isLoading;
      }, "LocalFirstAuth"),
    );
  }
}
