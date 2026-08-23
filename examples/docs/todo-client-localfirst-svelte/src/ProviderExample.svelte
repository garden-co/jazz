<script lang="ts" module>
  import { type Component } from "svelte";

  // Just a placeholder to make the TS compiler happy.
  const YourApp: Component = null as any;
</script>

<!-- #region provider-svelte -->
<script lang="ts">
  import { JazzSvelteProvider, type JazzContext } from "jazz-tools/svelte";

  const config = {
    appId: "my-app",
  };
</script>

<JazzSvelteProvider {config}>
  {#snippet children({ db }: { db: NonNullable<JazzContext["db"]> })}
    <YourApp />
  {/snippet}
  {#snippet fallback()}
    <p>Loading...</p>
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion provider-svelte -->
