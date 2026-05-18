<script lang="ts">
  import "../app.css";
  import { createJazzClient, JazzSvelteProvider, LocalFirstAuth } from "jazz-tools/svelte";
  import { env } from "$env/dynamic/public";

  let { children: pageChildren } = $props();

  const auth = new LocalFirstAuth();

  const appId = env.PUBLIC_JAZZ_APP_ID;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;

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

  let client = $derived(
    !auth.isLoading && auth.secret && appId && serverUrl
      ? createJazzClient({ appId, serverUrl, secret: auth.secret })
      : null,
  );
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
