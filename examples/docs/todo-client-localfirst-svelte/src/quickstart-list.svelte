<script lang="ts">
  import { QuerySubscription } from 'jazz-tools/svelte';
  import { app } from '../schema.js';
  import TodoItem from './TodoItem.svelte';
  import AddTodo from './AddTodo.svelte';

  const todos = new QuerySubscription(app.todos);
</script>

<ul>
  {#each todos.current ?? [] as todo (todo.id)}
    <TodoItem id={todo.id} />
  {/each}
</ul>
<AddTodo />

