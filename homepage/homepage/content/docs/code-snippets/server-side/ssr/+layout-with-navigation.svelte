<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { beforeNavigate, goto } from "$app/navigation"; // [!code ++]
  import { apiKey } from "$lib/apiKey";
  import { SyncConfig } from "jazz-tools";

  let { children } = $props();
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync} enableSSR navigation={{ beforeNavigate, goto }}> <!--[!code ++]-->
  {@render children?.()}
</JazzSvelteProvider>
