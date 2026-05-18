<script lang="ts">
  import {
    createJazzClient,
    JazzSvelteProvider,
    LocalFirstAuth,
  } from "jazz-tools/svelte";
  import type { AuthState } from "jazz-tools";
  import type { Snippet } from "svelte";
  import { env } from "$env/dynamic/public";
  import { getToken } from "$lib/auth-client";

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
    void getToken().then((token) => {
      if (!cancelled) jwtToken = token;
    });
    return () => {
      cancelled = true;
    };
  });

  let client = $derived.by(() => {
    if (!appId || !serverUrl) return null;
    if (authenticated) {
      return jwtToken ? createJazzClient({ appId, serverUrl, jwtToken }) : null;
    }
    return !auth.isLoading && auth.secret
      ? createJazzClient({ appId, serverUrl, secret: auth.secret })
      : null;
  });

  $effect(() => {
    if (!client || !authenticated) return;
    const currentClient = client;
    let cancelled = false;
    let unsubRefresh: (() => void) | undefined;
    void (async () => {
      const resolved = await currentClient;
      if (cancelled) return;
      unsubRefresh = resolved.db.onAuthChanged(async (state: AuthState) => {
        if (state.error !== "expired") return;
        const fresh = await getToken();
        if (fresh) resolved.db.updateAuthToken(fresh);
      });
    })();
    return () => {
      cancelled = true;
      unsubRefresh?.();
    };
  });
</script>

{#if client}
  <JazzSvelteProvider {client}>
    {#snippet children()}
      {@render pageChildren?.()}
    {/snippet}
    {#snippet fallback()}
      <p>Loading...</p>
    {/snippet}
  </JazzSvelteProvider>
{/if}
