<!-- #region reading-loading-state-svelte -->
<script lang="ts">
  import { QuerySubscription } from 'jazz-tools/svelte';
  import { app } from '../schema.js';

  const todos = new QuerySubscription(app.todos);
  // .current: undefined = not yet connected; [] = connected, no rows; [...] = rows present
</script>

{#if todos.current === undefined}
  <p>Connecting…</p>
{:else}
  <ul>
    {#each todos.current as todo}
      <li>{todo.title}</li>
    {/each}
  </ul>
{/if}
<!-- #endregion reading-loading-state-svelte -->
