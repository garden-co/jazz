<script lang="ts">
  import {
    JazzSvelteProvider,
    LocalFirstAuth,
  } from "jazz-tools/svelte";
  import type { Snippet } from "svelte";
  import { env } from "$env/dynamic/public";
  import { getToken } from "$lib/auth-client";
  import JazzTokenRefresh from "$lib/JazzTokenRefresh.svelte";

  let {
    authenticated,
    children: pageChildren,
  }: { authenticated: boolean; children: Snippet } = $props();

  const appId = env.PUBLIC_JAZZ_APP_ID;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;

  const auth = new LocalFirstAuth();
  let jwtToken = $state<string | null>(null);

  $effect(() => {
    if (!appId || !serverUrl) {
      const missing = [
        !appId && "PUBLIC_JAZZ_APP_ID",
        !serverUrl && "PUBLIC_JAZZ_SERVER_URL",
      ]
        .filter((v) => !!v)
        .join(" & ");
      console.error(
        `${missing} not set — the jazzSvelteKit() plugin should inject these.`,
      );
    }
  });

  $effect(() => {
    if (!authenticated) {
      jwtToken = null;
      return;
    }
    let cancelled = false;
    getToken().then((token) => {
      if (!cancelled) jwtToken = token;
    });
    return () => {
      cancelled = true;
    };
  });

  let config = $derived.by(() => {
    if (!appId || !serverUrl) return null;
    if (authenticated) {
      return jwtToken ? { appId, serverUrl, jwtToken } : null;
    }
    return !auth.isLoading && auth.secret
      ? { appId, serverUrl, secret: auth.secret }
      : null;
  });
</script>

{#if config}
  <JazzSvelteProvider {config}>
    {#snippet children({ db })}
      {#if authenticated}
        <JazzTokenRefresh {db} />
      {/if}
      {@render pageChildren?.()}
    {/snippet}
    {#snippet fallback()}
      <p>Loading...</p>
    {/snippet}
  </JazzSvelteProvider>
{/if}
