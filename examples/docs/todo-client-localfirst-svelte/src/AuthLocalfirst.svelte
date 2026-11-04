<!-- #region auth-localfirst-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import type { JazzClientConfig } from "jazz-tools/svelte";
  import { JazzSessionProvider } from "jazz-tools/svelte";
  let { config, children }: { config: JazzClientConfig; children: Snippet } = $props();
</script>
<JazzSessionProvider config={{ ...config, initial: "local-first" }}>{@render children()}</JazzSessionProvider>
<!-- #endregion auth-localfirst-svelte -->
