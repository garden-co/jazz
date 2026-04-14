<script lang="ts">
  import { getDb, QuerySubscription } from "jazz-tools/svelte";
  import { app } from "$lib/schema";

  const db = getDb();
  const todos = new QuerySubscription(app.todos);

  let title = $state("");

  function add(e: SubmitEvent) {
    e.preventDefault();
    const trimmed = title.trim();
    if (!trimmed) return;
    db.insert(app.todos, { title: trimmed, done: false });
    title = "";
  }
</script>

<section class="todo-widget">
  <h2>Your todos</h2>
  <form onsubmit={add}>
    <input
      type="text"
      bind:value={title}
      placeholder="Add a task"
      aria-label="New todo"
    />
    <button type="submit">Add</button>
  </form>
  <ul>
    {#each todos.current ?? [] as todo (todo.id)}
      <li class={todo.done ? "done" : ""}>
        <label>
          <input
            type="checkbox"
            checked={todo.done}
            onchange={() => db.update(app.todos, todo.id, { done: !todo.done })}
          />
          <span>{todo.title}</span>
        </label>
        <button
          type="button"
          aria-label="Delete"
          onclick={() => db.delete(app.todos, todo.id)}
        >
          ×
        </button>
      </li>
    {/each}
  </ul>
</section>
