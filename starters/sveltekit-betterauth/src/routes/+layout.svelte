<script lang="ts">
  import "../app.css";
  import JazzClientProvider from "$lib/JazzClientProvider.svelte";

  let { children } = $props();
</script>

<JazzClientProvider>
  {#snippet children()}{@render children?.()}{/snippet}
</JazzClientProvider>
