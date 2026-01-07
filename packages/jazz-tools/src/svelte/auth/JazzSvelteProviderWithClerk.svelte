<!--
  @component
  A pre-configured Jazz provider that integrates with Clerk authentication.

  Use this component instead of `JazzSvelteProvider` when using Clerk for authentication.
  It handles:
  - Pre-loading Jazz credentials from Clerk before rendering children
  - Registering Clerk auth state listeners
  - Wiring up logout functionality to Clerk's signOut

  @example
  ```svelte
  <JazzSvelteProviderWithClerk
    clerk={$clerk}
    sync={{ peer: "wss://cloud.jazz.tools/?key=your-key" }}
    AccountSchema={MyAccountSchema}
  >
    <App />
  </JazzSvelteProviderWithClerk>
  ```

  @category Auth Providers
-->
<script
  lang="ts"
  generics="S extends (AccountClass<Account> & CoValueFromRaw<Account>) | AnyAccountSchema"
>
  import {
    Account,
    type AccountClass,
    type AnyAccountSchema,
    type CoValueFromRaw,
    InMemoryKVStore,
    type InstanceOfSchema,
    JazzClerkAuth,
    KvStoreContext,
    type MinimalClerkClient,
    type SyncConfig,
  } from "jazz-tools";
  import {
    type BaseBrowserContextOptions,
    LocalStorageKVStore,
  } from "jazz-tools/browser";
  import type { Snippet } from "svelte";
  import { JazzSvelteProvider } from "../jazz.svelte.js";
  import RegisterClerkAuth from "./RegisterClerkAuth.svelte";

  type Props = {
    /** The Clerk client instance for authentication (can be null while Clerk is initializing) */
    clerk: MinimalClerkClient | null;
    /** Content to render when provider is initialized */
    children?: Snippet;
    /** Content to render while Clerk auth is loading */
    fallback?: Snippet;
    /** Content to render when authentication initialization fails */
    errorFallback?: Snippet<[Error]>;
    /** Callback when authentication initialization fails */
    onAuthError?: (error: Error) => void;
    /** Enable server-side rendering support with anonymous fallback */
    enableSSR?: boolean;
    /** Custom key for storing auth secrets in KvStore */
    authSecretStorageKey?: string;
    /** Jazz sync configuration (peer URL and key) */
    sync: SyncConfig;
    /** Custom storage implementation for auth secrets */
    storage?: BaseBrowserContextOptions["storage"];
    /** Account schema class for typed account access */
    AccountSchema?: S;
    /** Default profile name for new accounts */
    defaultProfileName?: string;
    /** Callback when an anonymous account is discarded during sign-in */
    onAnonymousAccountDiscarded?: (
      anonymousAccount: InstanceOfSchema<S>,
    ) => Promise<void>;
  };

  let {
    clerk,
    children,
    fallback,
    errorFallback,
    onAuthError,
    ...providerProps
  }: Props = $props();

  let isLoaded = $state(false);
  let initError = $state<Error | null>(null);

  function setupKvStore() {
    const isSSR = typeof window === "undefined";
    if (isSSR) {
      console.debug("[Jazz] Using InMemoryKVStore for SSR context");
    }
    KvStoreContext.getInstance().initialize(
      isSSR ? new InMemoryKVStore() : new LocalStorageKVStore(),
    );
  }

  /**
   * Pre-loads Jazz authentication data from Clerk before mounting JazzSvelteProvider.
   *
   * For authenticated Clerk users with existing Jazz credentials, this populates the auth
   * secret storage before rendering children, avoiding a flash of unauthenticated state.
   * For unauthenticated users, the initialization completes immediately with no effect.
   */
  $effect(() => {
    setupKvStore();

    if (!clerk) return;

    let cancelled = false;

    JazzClerkAuth.initializeAuth(clerk)
      .then(() => {
        if (!cancelled) {
          isLoaded = true;
          initError = null;
        }
      })
      .catch((error) => {
        console.error(
          "[Jazz] Clerk authentication initialization failed:",
          error,
        );
        if (!cancelled) {
          const errorObj =
            error instanceof Error ? error : new Error(String(error));
          initError = errorObj;
          onAuthError?.(errorObj);
        }
      });

    return () => {
      cancelled = true;
    };
  });
</script>

{#if initError}
  {#if errorFallback}
    {@render errorFallback(initError)}
  {:else}
    <div data-testid="jazz-clerk-auth-error">
      Authentication initialization failed. Please refresh the page or try again
      later.
    </div>
  {/if}
{:else if isLoaded && clerk}
  <JazzSvelteProvider {...providerProps} onLogOut={clerk.signOut}>
    <RegisterClerkAuth {clerk}>
      {@render children?.()}
    </RegisterClerkAuth>
  </JazzSvelteProvider>
{:else if fallback}
  {@render fallback?.()}
{/if}
