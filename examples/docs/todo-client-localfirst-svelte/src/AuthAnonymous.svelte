<!-- #region auth-anon-svelte -->
<script lang="ts">
  import { JazzSvelteProvider, type JazzContext } from 'jazz-tools/svelte';

  const config = {
    appId: 'my-app',
    serverUrl: 'http://127.0.0.1:4200',
  };
</script>

<JazzSvelteProvider {config}>
  {#snippet children({ db }: { db: NonNullable<JazzContext["db"]> })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-anon-svelte -->
