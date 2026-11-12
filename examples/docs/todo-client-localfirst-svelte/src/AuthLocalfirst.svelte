<!-- #region auth-localfirst-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { JazzProvider, type JazzClientConfig } from "jazz-tools/svelte";
  let { config, children }: { config: JazzClientConfig; children: Snippet } = $props();
</script>
<JazzProvider config={{ ...config, initial: "device" }}>{@render children()}</JazzProvider>
<!-- #endregion auth-localfirst-svelte -->
