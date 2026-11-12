<!-- #region provider-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { JazzProvider, type JazzClientConfig } from "jazz-tools/svelte";
  let { config, children }: { config: JazzClientConfig; children: Snippet } = $props();
</script>
<JazzProvider {config}>{@render children()}</JazzProvider>
<!-- #endregion provider-svelte -->
