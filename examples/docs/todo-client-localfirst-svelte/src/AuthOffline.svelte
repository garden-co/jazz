<!-- #region auth-offline-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';

  const client = createJazzClient({ appId: 'my-app' });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-offline-svelte -->
