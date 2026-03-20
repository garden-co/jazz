<script lang="ts">
  import { getDb, QuerySubscription } from 'jazz-tools/svelte';
  import { app } from '../schema/app.js';

  const { id }: { id: string } = $props();

  const db = getDb();
  const todos = new QuerySubscription(app.todos.where({ id }).limit(1));
  const todo = $derived(todos.current?.[0]);
</script>

{#if todo}
  <li class={todo.done ? 'done' : ''}>
    <input
      type="checkbox"
      checked={todo.done}
      onchange={() => db.update(app.todos, id, { done: !todo.done })}
    />
    <span>{todo.title}</span>
    <button onclick={() => db.delete(app.todos, id)}>&times;</button>
  </li>
{/if}
