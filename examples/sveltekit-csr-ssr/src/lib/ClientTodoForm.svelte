<script lang="ts">
  import { getJazzContext } from "jazz-tools/svelte";
  import { app } from "$lib/schema";

  const ctx = getJazzContext();

  function handleSubmit(event: SubmitEvent) {
    event.preventDefault();
    const form = event.currentTarget as HTMLFormElement;
    const title = (new FormData(form).get("titleField") as string | null)?.trim();
    if (!title || !ctx.db) return;
    ctx.db.insert(app.todos, { title, done: false });
    form.reset();
  }
</script>

<form onsubmit={handleSubmit} class="flex gap-2">
  <input
    name="titleField"
    type="text"
    placeholder="New todo…"
    class="flex-1 text-sm bg-transparent border border-foreground/15 rounded px-3 py-1.5 outline-none focus:border-foreground/40 placeholder:text-foreground/25"
  />
  <button
    type="submit"
    class="text-sm px-3 py-1.5 border border-foreground/15 rounded hover:bg-foreground/5 transition-colors cursor-pointer"
  >
    Add
  </button>
</form>
