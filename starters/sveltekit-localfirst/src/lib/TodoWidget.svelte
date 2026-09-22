<script lang="ts">
  import { getDb, QuerySubscription } from "jazz-tools/svelte";
  import { app } from "$lib/schema";

  const db = getDb();
  const todos = new QuerySubscription(app.todos);
  let localSaveState = "Ready to save locally";
  let latestSaveGeneration = 0;
  let pendingSaveCount = 0;
  let latestSaveFailed = false;

  function renderLocalSaveState() {
    localSaveState = latestSaveFailed
      ? "Save failed locally"
      : pendingSaveCount > 0
        ? "Saving locally…"
        : "Saved locally";
  }

  async function add(e: SubmitEvent) {
    e.preventDefault();
    const form = e.currentTarget as HTMLFormElement;
    const title = (new FormData(form).get("title") as string).trim();
    if (!title) return;
    const generation = ++latestSaveGeneration;
    pendingSaveCount += 1;
    latestSaveFailed = false;
    renderLocalSaveState();
    try {
      const write = db.insert(app.todos, { title, done: false });
      await write.wait({ tier: "local" });
      if (generation === latestSaveGeneration) form.reset();
    } catch {
      if (generation === latestSaveGeneration) latestSaveFailed = true;
    } finally {
      pendingSaveCount -= 1;
      if (generation === latestSaveGeneration || pendingSaveCount === 0) renderLocalSaveState();
    }
  }
</script>

<section class="todo-widget">
  <h2>Your todos</h2>
  <form onsubmit={add}>
    <input
      type="text"
      name="title"
      placeholder="Add a task"
      aria-label="New todo"
    />
    <button type="submit">Add</button>
  </form>
  <p role="status" aria-live="polite">{localSaveState}</p>
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
