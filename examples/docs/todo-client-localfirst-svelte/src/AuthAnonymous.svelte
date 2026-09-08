<!-- #region auth-anon-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import type { DbConfig } from "jazz-tools";
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  // Prepare a handle outside this context with createAccountManager.
  let { config, children }: { config: DbConfig; children: Snippet } = $props();
</script>
<JazzSvelteProvider {config}>{@render children()}</JazzSvelteProvider>
<!-- #endregion auth-anon-svelte -->
