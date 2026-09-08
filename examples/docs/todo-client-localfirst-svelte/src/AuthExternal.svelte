<!-- #region auth-external-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { createAccountManager, type DbConfig } from "jazz-tools";
  import { JazzSvelteProvider, accountState } from "jazz-tools/svelte";
  let { accounts, getToken, config, children }: {
    accounts: Awaited<ReturnType<typeof createAccountManager>>;
    getToken: () => Promise<string>;
    config: Omit<DbConfig, "account">;
    children: Snippet;
  } = $props();
  // Keep this app-owned manager stable for the lifetime of the login screen.
  const selection = $derived(accountState(accounts));
  // Login starts outside a context. Before linking, await the old context's
  // shutdown({ waitForSync: true }), then call accounts.linkJWT outside it.
  function signIn() { void accounts.loginJWT({ getToken }).catch(() => {}); }
</script>
{#if $selection.account}
  <JazzSvelteProvider config={{ ...config, account: $selection.account }}>{@render children()}</JazzSvelteProvider>
{:else}
  <button disabled={!!$selection.pending} onclick={signIn}>Sign in</button>
  {#if $selection.error}<p role="alert">{$selection.error.message}</p>{/if}
{/if}
<!-- #endregion auth-external-svelte -->
