<!-- Renders nothing on the server; after mount it always reads "client". -->
<script lang="ts">
  import { onMount } from "svelte";
  import Chip from "./Chip.svelte";

  let mounted = $state(false);
  onMount(() => {
    mounted = true;
  });
</script>

{#if mounted}
  <Chip renderedOn="client" />
{/if}
