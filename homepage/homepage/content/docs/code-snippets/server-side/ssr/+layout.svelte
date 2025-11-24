<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { apiKey } from "$lib/apiKey";
  import { SyncConfig } from "jazz-tools";

  let { children } = $props();
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<!--[!code ++:1]-->
<JazzSvelteProvider {sync} enableSSR>
  {@render children?.()}
</JazzSvelteProvider>
