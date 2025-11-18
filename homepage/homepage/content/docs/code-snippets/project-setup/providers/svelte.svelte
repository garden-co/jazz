<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { MyAppAccount } from "./schema";
  let { children } = $props();
</script>

<JazzSvelteProvider
  sync={{
    peer: "wss://cloud.jazz.tools/?key=your-api-key",
    when: "always" // When to sync: "always", "never", or "signedUp"
  }}
  AccountSchema={MyAppAccount}
>
  {@render children()}
</JazzSvelteProvider>
