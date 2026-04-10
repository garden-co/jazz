<!-- #region auth-external-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider } from "jazz-tools/svelte";

  const appId = "my-app";
  const jazzServerUrl = "http://127.0.0.1:4200";

  let jwtToken = $state<string | undefined>();

  const client = $derived(
    createJazzClient({
      appId,
      serverUrl: jazzServerUrl,
      jwtToken,
    }),
  );
</script>

{#key jwtToken}
  <JazzSvelteProvider {client}>
    {#snippet children({ db })}
      <!-- Your app content here -->
    {/snippet}
  </JazzSvelteProvider>
{/key}
<!-- #endregion auth-external-svelte -->
