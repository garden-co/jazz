<script lang="ts" module>
  import { type Component } from "svelte";

  // Just a placeholder to make the TS compiler happy.
  const YourApp: Component = null as any;
</script>

<!-- #region provider-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider, type JazzContext } from "jazz-tools/svelte";

  const client = createJazzClient({
    asyncSubscriptionsOnly: false,
    appId: "my-app",
  });
</script>

<JazzSvelteProvider {client}>
  {#snippet children({ db }: { db: NonNullable<JazzContext["db"]> })}
    <YourApp />
  {/snippet}
  {#snippet fallback()}
    <p>Loading...</p>
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion provider-svelte -->
