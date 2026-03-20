<!-- #region reading-conditional-query-svelte -->
<script lang="ts">
  import { QuerySubscription } from 'jazz-tools/svelte';
  import { app } from '../schema/app.js';

  let filter = $state<string | null>(null);

  const filtered = new QuerySubscription(
    () => filter ? app.todos.where({ title: { contains: filter } }) : undefined,
  );
</script>

<input bind:value={filter} placeholder="Filter by title" />
{#if filtered.current}
  <ul>
    {#each filtered.current as todo}
      <li>{todo.title}</li>
    {/each}
  </ul>
{/if}
<!-- #endregion reading-conditional-query-svelte -->
