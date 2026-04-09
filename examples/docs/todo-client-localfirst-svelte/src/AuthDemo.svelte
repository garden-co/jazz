<!-- #region auth-self-signed-svelte -->
<script lang="ts">
  import {
    createJazzClient,
    JazzSvelteProvider,
  } from 'jazz-tools/svelte';
  import { loadOrCreateIdentitySeed, mintSelfSignedToken } from 'jazz-tools';

  const appId = 'my-app';
  const seed = loadOrCreateIdentitySeed(appId);
  const jwtToken = mintSelfSignedToken(seed.seed, appId);

  const client = createJazzClient({
    appId,
    serverUrl: 'http://127.0.0.1:4200',
    jwtToken,
  });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db })}
    <slot />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion auth-self-signed-svelte -->
