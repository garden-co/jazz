<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  const { children } = $props();
  // [!code hide]
  const apiKey = "your_api_key_here";
</script>

<JazzSvelteProvider
  sync={{
  peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  // This makes the app work in local mode when using Anonymous Authentication
  when: "signedUp"
}}>
  {@render children?.()}
</JazzSvelteProvider>
