<!-- #region auth-self-signed-token-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
  import { loadOrCreateIdentitySeed, mintSelfSignedToken } from 'jazz-tools';

  const appId = 'my-app';
  const seed = loadOrCreateIdentitySeed(appId);
  const jwtToken = mintSelfSignedToken(seed.seed, appId);

  const client = createJazzClient({
    appId,
    jwtToken,
  });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-self-signed-token-svelte -->
