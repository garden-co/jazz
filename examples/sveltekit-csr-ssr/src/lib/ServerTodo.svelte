<script lang="ts">
  import ServerTodoForm from "./ServerTodoForm.svelte";

  let { todos }: { todos: Array<{ id: string; title: string }> } = $props();
</script>

<ServerTodoForm />
<ul class="mt-4 space-y-1">
  {#if todos.length === 0}
    <li class="text-sm text-foreground/30 italic">No todos yet.</li>
  {/if}
  {#each todos as todo (todo.id)}
    <li class="text-sm py-1.5 border-b border-foreground/5 last:border-0">{todo.title}</li>
  {/each}
</ul>
