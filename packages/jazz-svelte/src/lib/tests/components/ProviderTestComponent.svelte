<script lang="ts">
  import { JazzProvider } from "$lib/jazz.svelte.js";

  const { auth } = $props();
</script>

<div data-testid="provider-test">
  <JazzProvider {auth} peer="wss://cloud.jazz.tools/?key=jazz-svelte-test">
    {#if auth}
      <span data-testid="provider-auth-test">{JSON.stringify(auth)}</span>
    {/if}
  </JazzProvider>
</div>
