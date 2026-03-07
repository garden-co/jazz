<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { JazzFestAccount } from "$lib/schema";
  // @ts-expect-error No real env vars
  import { PUBLIC_JAZZ_API_KEY } from "$env/static/public";
  import type { SyncConfig } from "jazz-tools";

  const apiKey = PUBLIC_JAZZ_API_KEY;
  let { children } = $props();
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync} AccountSchema={JazzFestAccount}>
  {@render children?.()}
</JazzSvelteProvider>
