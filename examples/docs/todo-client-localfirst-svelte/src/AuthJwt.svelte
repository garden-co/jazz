<!-- #region auth-jwt-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';

  const client = createJazzClient({
    appId: 'my-app',
    serverUrl: 'http://127.0.0.1:4200',
    jwtToken: '<provider-jwt>',
  });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-jwt-svelte -->
