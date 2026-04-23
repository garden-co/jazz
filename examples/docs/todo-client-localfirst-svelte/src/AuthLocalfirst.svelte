<!-- #region auth-localfirst-svelte -->
<script lang="ts">
  import { onMount } from 'svelte';
  import { BrowserAuthSecretStore, createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';

  let client = $state<ReturnType<typeof createJazzClient> | null>(null);

  onMount(async () => {
    const secret = await BrowserAuthSecretStore.getOrCreateSecret();
    client = createJazzClient({
      appId: 'my-app',
      secret,
    });
  });
</script>

{#if client}
  <JazzSvelteProvider {client}>
    {#snippet children({ db })}
      <slot />
    {/snippet}
  </JazzSvelteProvider>
{/if}
<!-- #endregion auth-localfirst-svelte -->
