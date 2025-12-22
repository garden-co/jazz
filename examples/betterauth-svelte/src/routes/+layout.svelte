<script lang="ts">
  import { Toaster } from "$lib/components/ui/sonner/index.js";

  import "./layout.css";
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import { apiKey } from "$lib/apiKey";
  import type { SyncConfig } from "jazz-tools";
  import { betterAuthClient } from "$lib/auth-client";
  import AuthProvider from "jazz-tools/better-auth/auth/svelte";

  let { children } = $props();
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<!-- Add the enableSSR flag to allow Jazz to render data server side -->
<JazzSvelteProvider {sync} enableSSR>
  <AuthProvider {betterAuthClient} />
  <Toaster richColors />

  {@render children?.()}
</JazzSvelteProvider>
