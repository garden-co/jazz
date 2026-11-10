<script lang="ts">
  import { QuerySubscription } from 'jazz-tools/svelte';
  import { app } from '../schema.js';

  const { id }: { id: string } = $props();

  const todos = new QuerySubscription(() => app.todos.where({ id }).limit(1));
  const todo = $derived(todos.current?.[0]);
</script>

{#if todo}
  <li class={todo.done ? 'done' : ''}>
    <input
      type="checkbox"
      checked={todo.done}
      onchange={() => todos.db.update(app.todos, id, { done: !todo.done })}
    />
    <span>{todo.title}</span>
    <button onclick={() => todos.db.delete(app.todos, id)}>&times;</button>
  </li>
{/if}
