<script lang="ts">
  import { SyncConfig } from "jazz-tools";
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  // [!code hide]
  const apiKey = "";
  let { children } = $props();
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync} enableSSR>
  {@render children?.()}
</JazzSvelteProvider>
