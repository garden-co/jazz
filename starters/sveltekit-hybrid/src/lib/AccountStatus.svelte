<script lang="ts">
  import { getJazzSession } from "jazz-tools/svelte";
  import { credential } from "$lib/accounts";
  const jazz = getJazzSession();
</script>
{#if $jazz.error}
  <aside class="alert-error" role="alert">
    {$jazz.error.message}
    {#if $jazz.status === "error"}<button type="button" onclick={() => jazz.retry().catch(() => {})}>Retry startup</button>
    {:else}<button type="button" onclick={() => jazz.linkJWT({ getToken: credential }).catch(() => {})}>Retry linking</button>{/if}
  </aside>
{:else if $jazz.status !== "ready"}<p>Loading...</p>{/if}
