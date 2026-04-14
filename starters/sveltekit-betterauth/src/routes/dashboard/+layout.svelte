<script lang="ts">
  import { onMount } from "svelte";
  import { createJazzClient, JazzSvelteProvider } from "jazz-tools/svelte";
  import type { DbConfig } from "jazz-tools";
  import { env } from "$env/dynamic/public";
  import { getToken } from "$lib/auth-client";

  let { children: pageChildren } = $props();
  let client = $state<ReturnType<typeof createJazzClient> | null>(null);

  onMount(async () => {
    const token = await getToken();
    if (!token) return;
    const appId = env.PUBLIC_JAZZ_APP_ID;
    if (!appId) {
      console.error(
        "PUBLIC_JAZZ_APP_ID is not set — the jazzSvelteKit() plugin should inject it.",
      );
      return;
    }
    const config: DbConfig = {
      appId,
      serverUrl: env.PUBLIC_JAZZ_SERVER_URL ?? "ws://localhost:1625",
      env: "dev",
      userBranch: "main",
      driver: { type: "memory" },
      jwtToken: token,
    };
    client = createJazzClient(config);
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
