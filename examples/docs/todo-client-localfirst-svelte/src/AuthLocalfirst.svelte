<!-- #region auth-localfirst-svelte -->
<script lang="ts">
  import {
    LocalFirstAuth,
    createJazzClient,
    JazzSvelteProvider,
  } from 'jazz-tools/svelte';
  import type { Snippet } from 'svelte';

  let { children }: { children: Snippet } = $props();

  const auth = new LocalFirstAuth();

  let client = $derived(
    !auth.isLoading && auth.secret
      ? createJazzClient({ appId: 'my-app', secret: auth.secret })
      : null,
  );
</script>

{#if client}
  <JazzSvelteProvider {client}>
    {@render children()}
  </JazzSvelteProvider>
{/if}
<!-- #endregion auth-localfirst-svelte -->
