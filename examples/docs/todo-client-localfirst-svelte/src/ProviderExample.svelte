<script lang="ts" module>
  import { type Component } from "svelte";

  // Just a placeholder to make the TS compiler happy.
  const YourApp: Component = null as any;
</script>

<!-- #region provider-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider } from "jazz-tools/svelte";

  const client = createJazzClient({
    appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
  });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db })}
    <YourApp />
  {/snippet}
  {#snippet fallback()}
    <p>Loading...</p>
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion provider-svelte -->
