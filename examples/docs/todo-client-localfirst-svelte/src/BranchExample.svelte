<script lang="ts">
  // #region branch-svelte
  import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';

  const client = createJazzClient({
    asyncSubscriptionsOnly: false,
    appId: 'my-app',
    env: 'prod',
    userBranch: 'staging',
  });
  // #endregion branch-svelte
</script>

<JazzSvelteProvider {client}>
  {#snippet children()}
    <!-- Insert your app's main component -->
  {/snippet}
</JazzSvelteProvider>
