<script lang="ts">
  import { getJazzSession } from "jazz-tools/svelte";
  const jazz = getJazzSession();
</script>
{#if $jazz.error}<p role="alert">{$jazz.error.message}</p><button onclick={() => jazz.retry().catch(() => {})}>Retry</button>
{:else}<p>Loading...</p>{/if}
