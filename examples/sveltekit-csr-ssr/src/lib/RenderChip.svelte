<script lang="ts">
  import { onMount } from "svelte";
  import Chip from "./Chip.svelte";

  let hydrated = $state(false);
  onMount(() => {
    hydrated = true;
  });
</script>

<Chip renderedOn={hydrated ? "hydrated" : "server"} />
