<!-- #region context-setup-svelte-runtime-sources -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import type { AccountHandle } from "jazz-tools";
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  // Prepare the handle with these same runtimeSources.
  let { account, children }: { account: AccountHandle; children: Snippet } = $props();
  const config = $derived({
    appId: "my-app", serverUrl: "https://my-jazz-server.example.com", account,
    runtimeSources: { baseUrl: "/assets/jazz/", wasmVersion: "2026-08-25" },
  });
</script>
<JazzSvelteProvider {config}>{@render children()}</JazzSvelteProvider>
<!-- #endregion context-setup-svelte-runtime-sources -->
