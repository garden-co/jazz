<script lang="ts">
  import "../app.css";
  import { onMount } from "svelte";
  import { createJazzClient, JazzSvelteProvider, BrowserAuthSecretStore } from "jazz-tools/svelte";
  import type { DbConfig } from "jazz-tools";
  import { env } from "$env/dynamic/public";

  let { children: pageChildren } = $props();

  let client = $state<ReturnType<typeof createJazzClient> | null>(null);

  onMount(() => {
    (async () => {
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
      const secret = await BrowserAuthSecretStore.getOrCreateSecret();
      const config: DbConfig = {
        appId,
        serverUrl,
        secret,
      };
      client = createJazzClient(config);
    })();
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
