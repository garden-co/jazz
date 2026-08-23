<!-- #region auth-localfirst-svelte -->
<script lang="ts">
  import {
    LocalFirstAuth,
    JazzSvelteProvider,
  } from 'jazz-tools/svelte';
  import type { Snippet } from 'svelte';

  let { children }: { children: Snippet } = $props();

  const auth = new LocalFirstAuth();

  let config = $derived(
    !auth.isLoading && auth.secret
      ? { appId: 'my-app', secret: auth.secret }
      : null,
  );
</script>

{#if config}
  <JazzSvelteProvider {config}>
    {@render children()}
  </JazzSvelteProvider>
{/if}
<!-- #endregion auth-localfirst-svelte -->
