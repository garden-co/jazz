<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  const { children } = $props();
  import { MyAppAccount } from "./schema";
  import { SyncConfig } from "jazz-tools";
  // [!code hide]
  const apiKey = "";

  // Example configuration for authentication and peer connection
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync} AccountSchema={MyAppAccount}>
  {@render children?.()}
</JazzSvelteProvider>
