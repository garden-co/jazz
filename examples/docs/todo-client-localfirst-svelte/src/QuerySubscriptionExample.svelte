<!-- #region query-subscription-svelte -->
<script lang="ts">
  import { QuerySubscription } from 'jazz-tools/svelte';
  import { app } from '../schema.js';

  const todos = new QuerySubscription(
    app.todos.where({ done: false }),
  );
</script>

{#each todos.current ?? [] as todo}
  <li>{todo.title}</li>
{/each}
<!-- #endregion query-subscription-svelte -->
