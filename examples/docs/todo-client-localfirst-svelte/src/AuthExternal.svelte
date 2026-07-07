<!-- #region auth-external-svelte -->
<script lang="ts">
  import {
    createJazzClient,
    JazzSvelteProvider,
    type JazzContext,
  } from "jazz-tools/svelte";

  const appId = "my-app";
  const jazzServerUrl = "http://127.0.0.1:4200";

  let jwtToken = $state<string | undefined>();

  function onSignedIn(providerJwt: string) {
    jwtToken = providerJwt;
  }

  const client = $derived(
    createJazzClient({
      asyncSubscriptionsOnly: false,
      appId,
      serverUrl: jazzServerUrl,
      jwtToken,
    }),
  );
</script>

{#key jwtToken}
  <JazzSvelteProvider {client}>
    {#snippet children({ db }: { db: NonNullable<JazzContext["db"]> })}
      <button onclick={() => onSignedIn("<provider-jwt>")}>Sign in</button>
      <!-- Your app content here -->
    {/snippet}
  </JazzSvelteProvider>
{/key}
<!-- #endregion auth-external-svelte -->
