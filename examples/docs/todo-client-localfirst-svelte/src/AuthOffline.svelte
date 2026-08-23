<!-- #region auth-offline-svelte -->
<script lang="ts">
  import { JazzSvelteProvider, type JazzContext } from 'jazz-tools/svelte';

  const config = { appId: 'my-app' };
</script>

<JazzSvelteProvider {config}>
  {#snippet children({ db }: { db: NonNullable<JazzContext["db"]> })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-offline-svelte -->
