<!-- #region context-setup-svelte-runtime-sources -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { JazzProvider, type JazzClientConfig } from "jazz-tools/svelte";
  let { children }: { children: Snippet } = $props();
  const config: JazzClientConfig = {
    serverUrl: "https://my-jazz-server.example.com",
    initial: "device",
    runtimeSources: { baseUrl: "/assets/jazz/", wasmVersion: "2026-08-25" },
  };
</script>
<JazzProvider {config}>{@render children()}</JazzProvider>
<!-- #endregion context-setup-svelte-runtime-sources -->
