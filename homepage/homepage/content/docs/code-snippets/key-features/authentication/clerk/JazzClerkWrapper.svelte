<script lang="ts">
  // @ts-ignore svelte-clerk is not installed in the homepage project but it is in the monorepo
  import { useClerkContext } from "svelte-clerk";
  import { JazzSvelteProviderWithClerk } from "jazz-tools/svelte";

  const apiKey = "you@example.com";

  const ctx = useClerkContext();
  const clerk = $derived(ctx.clerk);

  let { children } = $props();
</script>

{#snippet loading()}
  <p>Loading...</p>
{/snippet}

<JazzSvelteProviderWithClerk
  {clerk}
  sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
  fallback={loading}
>
  {@render children?.()}
</JazzSvelteProviderWithClerk>
