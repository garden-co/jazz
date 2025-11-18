<script lang="ts">
  // [!code ++:1]
  import { JazzSvelteProvider, PasskeyAuthBasicUI } from "jazz-tools/svelte";
  import { JazzFestAccount } from "$lib/schema";
  // @ts-expect-error This is available in a real app.
  import { PUBLIC_JAZZ_API_KEY } from "$env/static/public";

  const apiKey = PUBLIC_JAZZ_API_KEY;
  let { children } = $props();
  const sync = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync} AccountSchema={JazzFestAccount}>
  <!-- [!code ++:1] -->
  <PasskeyAuthBasicUI appName="JazzFest">
    {@render children?.()}
  </PasskeyAuthBasicUI>
</JazzSvelteProvider>
