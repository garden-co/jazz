<script lang="ts">
  import { getJazzSession } from "jazz-tools/svelte";
  import { credential } from "$lib/accounts";
  const jazz = getJazzSession();
</script>
{#if $jazz.error}
  <aside class="alert-error" role="alert">
    {$jazz.error.message}
    <button type="button" onclick={() => jazz.linkJWT({ getToken: credential }).catch(() => {})}>Retry linking</button>
  </aside>
{/if}
