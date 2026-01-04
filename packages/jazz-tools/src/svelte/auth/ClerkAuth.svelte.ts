import { JazzClerkAuth, type MinimalClerkClient } from "jazz-tools";
import { getAuthSecretStorage, getJazzContext } from "../jazz.svelte.js";
import { useIsAuthenticated } from "./useIsAuthenticated.svelte.js";

/**
 * Authentication state returned by {@link useClerkAuth}.
 *
 * - `"anonymous"`: User is not authenticated with Jazz (may or may not be signed into Clerk)
 * - `"signedIn"`: User is authenticated with both Clerk and Jazz
 */
export type ClerkAuth = {
  state: "anonymous" | "signedIn";
};

/**
 * Registers a Clerk authentication listener and provides reactive auth state.
 *
 * Must be used within a component that is a child of `JazzSvelteProvider`.
 * Automatically syncs Clerk authentication state with Jazz.
 *
 * @example
 * ```svelte
 * <script>
 *   import { useClerkAuth } from "jazz-tools/svelte";
 *
 *   const auth = useClerkAuth(clerk);
 * </script>
 *
 * {#if auth.state === "signedIn"}
 *   <p>Welcome back!</p>
 * {:else}
 *   <p>Please sign in</p>
 * {/if}
 * ```
 *
 * @param clerk - The Clerk client instance
 * @returns An object with a reactive `state` property
 * @throws Error if used in guest mode
 * @category Auth Providers
 */
export function useClerkAuth(clerk: MinimalClerkClient): ClerkAuth {
  const context = getJazzContext();
  const authSecretStorage = getAuthSecretStorage();

  if ("guest" in context.current) {
    throw new Error("Clerk auth is not supported in guest mode");
  }

  const authMethod = new JazzClerkAuth(
    context.current.authenticate,
    context.current.logOut,
    authSecretStorage,
  );

  $effect(() => {
    return authMethod.registerListener(clerk);
  });

  const isAuthenticated = useIsAuthenticated();
  const state = $derived(isAuthenticated.current ? "signedIn" : "anonymous");

  return {
    get state() {
      return state;
    },
  };
}
