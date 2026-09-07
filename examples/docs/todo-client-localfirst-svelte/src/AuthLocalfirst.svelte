<!-- #region auth-localfirst-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import type { JazzSessionConfig } from "jazz-tools/svelte";
  import { JazzSessionProvider } from "jazz-tools/svelte";
  let { config, children }: { config: JazzSessionConfig; children: Snippet } = $props();
</script>
<JazzSessionProvider config={{ ...config, initial: "local-first" }}>{@render children()}</JazzSessionProvider>
<!-- #endregion auth-localfirst-svelte -->
