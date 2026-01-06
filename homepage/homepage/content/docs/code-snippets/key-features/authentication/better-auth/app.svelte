<script lang="ts">
  import AuthProvider from "jazz-tools/better-auth/auth/svelte";
  import { betterAuthClient } from "@/lib/auth-client";
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { SyncConfig } from "jazz-tools";
  const { children } = $props();
  // [!code hide]
  const apiKey = "";

  // Example configuration for authentication and peer connection
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync}>
  <AuthProvider {betterAuthClient}>
    {@render children?.()}
  </AuthProvider>
</JazzSvelteProvider>
