<script lang="ts">
  import { getJazzSession } from '../../src/svelte/session-state.js';
  const jazz = getJazzSession();
</script>
<p>{$jazz.status}</p>
{#if $jazz.error}<button onclick={() => jazz.retry().catch(() => {})}>Retry</button>{/if}
