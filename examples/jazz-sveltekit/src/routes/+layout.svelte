<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { apiKey } from "$lib/apiKey";
  import type { SyncConfig } from "jazz-tools";
  import { beforeNavigate, goto } from "$app/navigation";

  let { children } = $props();
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<!-- Add the enableSSR flag to allow Jazz to render data server side -->
<JazzSvelteProvider {sync} enableSSR navigation={{ goto, beforeNavigate }}>
  <main class="container" style="margin-top: 2rem;">
    {@render children?.()}
  </main>
</JazzSvelteProvider>
