<script lang="ts">
  // #region branch-svelte
  import { JazzSvelteProvider } from 'jazz-tools/svelte';

  const config = {
    appId: 'my-app',
    env: 'prod',
    userBranch: 'staging',
  };
  // #endregion branch-svelte
</script>

<JazzSvelteProvider {config}>
  {#snippet children()}
    <!-- Insert your app's main component -->
  {/snippet}
</JazzSvelteProvider>
