<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  const { children } = $props();
  // [!code hide]
  const apiKey = "your_api_key_here";
</script>

<JazzSvelteProvider
  sync={{
  peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  // Controls when sync is enabled for
  // both Anonymous Authentication and Authenticated Account
  when: "always", // or "signedUp" or "never"
}}>
  {@render children?.()}
</JazzSvelteProvider>
