<!-- #region auth-localfirst-svelte -->
<script lang="ts">
  import { BrowserAuthSecretStore, createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
  import type { Snippet } from 'svelte';

  let { children }: { children: Snippet } = $props();

  const client = BrowserAuthSecretStore.getOrCreateSecret().then((secret) =>
    createJazzClient({ appId: 'my-app', secret }),
  );
</script>

<JazzSvelteProvider {client}>
  {@render children()}
</JazzSvelteProvider>
<!-- #endregion auth-localfirst-svelte -->
