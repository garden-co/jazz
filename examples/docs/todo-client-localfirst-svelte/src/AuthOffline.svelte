<!-- #region auth-offline-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider, type JazzContext } from 'jazz-tools/svelte';

  const client = createJazzClient({ asyncSubscriptionsOnly: false, appId: 'my-app' });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db }: { db: NonNullable<JazzContext["db"]> })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-offline-svelte -->
