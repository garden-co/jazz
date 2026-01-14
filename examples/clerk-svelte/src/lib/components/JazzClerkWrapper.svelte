<script lang="ts">
  import { useClerkContext } from 'svelte-clerk/client';
  import { JazzSvelteProviderWithClerk } from 'jazz-tools/svelte';
  import { apiKey } from '../../apiKey';

  const ctx = useClerkContext();
  const clerk = $derived(ctx.clerk);
  const isLoaded = $derived(ctx.isLoaded);

  let { children } = $props();
</script>

{#if isLoaded}
  <JazzSvelteProviderWithClerk {clerk} sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}>
    {@render children?.()}
  </JazzSvelteProviderWithClerk>
{/if}
