<script lang="ts">
  import { useClerkContext } from "svelte-clerk";
  import { JazzSvelteProviderWithClerk } from "jazz-tools/svelte";

  const apiKey = "you@example.com";

  const ctx = useClerkContext();
  const clerk = $derived(ctx.clerk);

  let { children } = $props();
</script>

<JazzSvelteProviderWithClerk
  {clerk}
  sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
>
  {@render children?.()}
</JazzSvelteProviderWithClerk>
