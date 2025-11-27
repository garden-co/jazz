<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  const { children } = $props();
  // [!code hide]
  const apiKey = "your_api_key_here";
</script>

<!-- Enable Guest Mode for public content -->
<JazzSvelteProvider
  guestMode={true}
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    when: "signedUp"
  }}>
  {@render children?.()}
</JazzSvelteProvider>
