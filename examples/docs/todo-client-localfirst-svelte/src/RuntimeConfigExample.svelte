<script lang="ts" module>
  import { type Component } from "svelte";

  // We don't bother writing a real dummy component here. This block sits outside
  // the docs #region below, so it's never shown to readers — it exists purely to
  // give `<YourApp />` a type so the snippet passes the type-check.
  const YourApp: Component = null as any;
</script>

<!-- #region context-setup-svelte-runtime-sources -->
<script lang="ts">
  import { JazzSvelteProvider, createJazzClient } from "jazz-tools/svelte";

  const client = createJazzClient({
    appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
    serverUrl: "https://my-jazz-server.example.com",
    runtimeSources: {
      baseUrl: "/assets/jazz/",
    },
  });
</script>

<JazzSvelteProvider {client}>
  {#snippet children()}
    <YourApp />
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion context-setup-svelte-runtime-sources -->
