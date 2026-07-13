<!-- #region auth-offline-svelte -->
<script lang="ts">
  import type { Db } from 'jazz-tools';
  import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';

  const client = createJazzClient({ appId: 'my-app' });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db }: { db: Db })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-offline-svelte -->
