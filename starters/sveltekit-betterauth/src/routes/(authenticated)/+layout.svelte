<script lang="ts">
  import { onMount } from "svelte";
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import type { DbConfig } from "jazz-tools";
  import { env } from "$env/dynamic/public";
  import { getToken } from "$lib/auth-client";
  import JazzTokenRefresh from "$lib/JazzTokenRefresh.svelte";

  let { children: pageChildren } = $props();
  let config = $state<DbConfig | null>(null);

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
      config = { appId, serverUrl, jwtToken: token };
    })();
  });
</script>

{#if config}
  <JazzSvelteProvider {config}>
    {#snippet children({ db })}
      <JazzTokenRefresh {db} />
      {@render pageChildren?.()}
    {/snippet}
    {#snippet fallback()}
      <p>Loading...</p>
    {/snippet}
  </JazzSvelteProvider>
{/if}
