<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  // [!code hide]
  const apiKey = "";
  let { children } = $props();
  const sync = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync} enableSSR>
  {@render children?.()}
</JazzSvelteProvider>
