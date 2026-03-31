<script lang="ts">
  // #region branch-svelte
  import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';

  const client = createJazzClient({
    appId: 'my-app',
    env: 'prod',
    userBranch: 'staging',
  });
  // #endregion branch-svelte
</script>

<JazzSvelteProvider {client}>
  <slot />
</JazzSvelteProvider>
