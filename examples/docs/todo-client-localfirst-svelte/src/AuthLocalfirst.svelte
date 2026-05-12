<!-- #region auth-localfirst-svelte -->
<script lang="ts">
  import { BrowserAuthSecretStore, createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';

  const client = BrowserAuthSecretStore.getOrCreateSecret().then((secret) =>
    createJazzClient({ appId: 'my-app', secret }),
  );
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-localfirst-svelte -->
