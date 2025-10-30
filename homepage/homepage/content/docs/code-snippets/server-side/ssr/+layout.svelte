<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { apiKey } from "$lib/apiKey";

  let { children } = $props();
  const sync = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<!--[!code ++:1]-->
<JazzSvelteProvider {sync} enableSSR>
  {@render children?.()}
</JazzSvelteProvider>
