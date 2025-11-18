<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  let { children } = $props();
  import { MyAppAccount } from "./schema";
  // [!code hide]
  const apiKey = "";

  // Example configuration for authentication and peer connection
  let sync = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync} AccountSchema={MyAppAccount}>
  {@render children?.()}
</JazzSvelteProvider>
