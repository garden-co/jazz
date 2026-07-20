<!-- #region auth-anon-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider, type JazzContext } from 'jazz-tools/svelte';

  const client = createJazzClient({
    asyncSubscriptionsOnly: false,
    appId: 'my-app',
    serverUrl: 'http://127.0.0.1:4200',
  });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db }: { db: NonNullable<JazzContext["db"]> })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-anon-svelte -->
