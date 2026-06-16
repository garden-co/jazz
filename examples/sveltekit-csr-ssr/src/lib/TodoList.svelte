<script lang="ts">
  import { QuerySubscription, type DehydratedSnapshot } from "jazz-tools/svelte";
  import { app } from "$lib/schema";

  // Set only in the hydrate column; the client-only column omits it. Seeds the
  // rows for the SSR render and first paint, then live sync takes over.
  let { snapshot }: { snapshot?: DehydratedSnapshot } = $props();

  const todos = new QuerySubscription(app.todos, () => ({ snapshot }));
</script>

<ul class="mt-4 space-y-1">
  {#if (todos.current ?? []).length === 0}
    <li class="text-sm text-foreground/30 italic">No todos yet.</li>
  {/if}
  {#each todos.current ?? [] as todo (todo.id)}
    <li class="text-sm py-1.5 border-b border-foreground/5 last:border-0">{todo.title}</li>
  {/each}
</ul>
