<script lang="ts">
  import { onMount } from "svelte";
  import { createJazzClient, JazzSvelteProvider } from "jazz-tools/svelte";
  import type { DbConfig } from "jazz-tools";
  import { env } from "$env/dynamic/public";
  import { getToken } from "$lib/auth-client";

  let { children: pageChildren } = $props();
  let client = $state<ReturnType<typeof createJazzClient> | null>(null);
  let unsubRefresh: (() => void) | undefined;

  onMount(() => {
    (async () => {
      const token = await getToken();
      if (!token) return;
      const appId = env.PUBLIC_JAZZ_APP_ID;
      const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
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
        return;
      }
      const config: DbConfig = { appId, serverUrl, jwtToken: token };
      client = createJazzClient(config);

      // Re-mint the Jazz JWT whenever Better Auth reports it as expired, so
      // long-lived sessions don't silently drop to unauthenticated.
      const resolved = await client;
      unsubRefresh = resolved.db.onAuthChanged(async (state) => {
        if (state.status !== "unauthenticated") return;
        const fresh = await getToken();
        if (fresh) resolved.db.updateAuthToken(fresh);
      });
    })();
    return () => unsubRefresh?.();
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
