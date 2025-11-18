<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  let { children } = $props();
  let apiKey = "Get a free API Key at dashboard.jazz.tools, or use your email as a temporary key.";
</script>

<JazzSvelteProvider
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  }}
>
  {@render children()}
</JazzSvelteProvider>
