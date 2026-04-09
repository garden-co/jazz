<!-- #region context-setup-svelte-runtime-sources -->
<script lang="ts">
  import { JazzProvider, createJazzClient } from "jazz-tools/svelte";

  const client = createJazzClient({
    appId: "my-app",
    serverUrl: "https://my-jazz-server.example.com",
    runtimeSources: {
      baseUrl: "/assets/jazz/",
    },
  });
</script>

<JazzProvider {client}>
  <!-- ... -->
</JazzProvider>
<!-- #endregion context-setup-svelte-runtime-sources -->
