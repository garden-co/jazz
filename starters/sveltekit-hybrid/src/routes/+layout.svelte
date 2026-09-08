<script lang="ts">
  import "../app.css";
  import JazzClientProvider from "$lib/JazzClientProvider.svelte";

  let { children: pageChildren } = $props();

</script>

<JazzClientProvider>
  {#snippet children()}
    {@render pageChildren?.()}
  {/snippet}
</JazzClientProvider>
