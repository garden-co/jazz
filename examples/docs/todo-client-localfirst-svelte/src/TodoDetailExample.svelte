<!-- #region reading-one-svelte -->
<script lang="ts">
  import { QuerySubscriptionOne } from 'jazz-tools/svelte';
  import { app } from '../schema.js';

  let { id }: { id: string } = $props();

  const todo = new QuerySubscriptionOne(() => app.todos.where({ id }));
  // .current: undefined = loading; null = no row matches or you can't read it
</script>

{#if todo.current === undefined}
  <p>Loading…</p>
{:else if todo.current === null}
  <p>Todo not found.</p>
{:else}
  <h1>{todo.current.title}</h1>
{/if}
<!-- #endregion reading-one-svelte -->
