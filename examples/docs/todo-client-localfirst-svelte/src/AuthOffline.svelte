<!-- #region auth-offline-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { JazzProvider } from "jazz-tools/svelte";
  let { children }: { children: Snippet } = $props();
  // No server: Jazz stays on this device, with one device account per browser profile.
</script>
<JazzProvider config={{ serverUrl: null, client: () => import("jazz-tools/local-first") }}>
  {@render children()}
</JazzProvider>
<!-- #endregion auth-offline-svelte -->
