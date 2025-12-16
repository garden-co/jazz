<script lang="ts">
import AuthProvider from "jazz-tools/better-auth/auth/svelte";
import { betterAuthClient } from "@/lib/auth-client";
</script>


<script lang="ts">
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  const { children } = $props();
  import { SyncConfig } from "jazz-tools";
  // [!code hide]
  const apiKey = "";

  // Example configuration for authentication and peer connection
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider>
  <AuthProvider betterAuthClient={betterAuthClient}>
    {@render children?.()}
  </AuthProvider>
</JazzSvelteProvider>
