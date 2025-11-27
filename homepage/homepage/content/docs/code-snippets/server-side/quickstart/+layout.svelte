<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  // @ts-expect-error Only available in a real SK app
	import { PUBLIC_JAZZ_API_KEY } from "$env/static/public";
  import { SyncConfig } from "jazz-tools";
  let { children } = $props();
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${PUBLIC_JAZZ_API_KEY}` };
</script>

<JazzSvelteProvider {sync}>
  {@render children?.()}
</JazzSvelteProvider>
