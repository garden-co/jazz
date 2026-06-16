<script lang="ts">
  import Chip from "$lib/Chip.svelte";
  import ClientChip from "$lib/ClientChip.svelte";
  import RenderChip from "$lib/RenderChip.svelte";
  import ServerTodo from "$lib/ServerTodo.svelte";
  import TodoPanel from "$lib/TodoPanel.svelte";
  import type { PageData } from "./$types";

  let { data }: { data: PageData } = $props();
</script>

{#snippet columnNote(text: string)}
  <details class="mb-4">
    <summary class="cursor-pointer select-none text-xs text-foreground/40 hover:text-foreground/60">
      What is this column doing?
    </summary>
    <p class="mt-2 text-xs text-foreground/50 leading-relaxed">{text}</p>
  </details>
{/snippet}

<main class="min-h-screen p-8 max-w-6xl w-full mx-auto">
  <h1 class="text-lg font-semibold mb-6 text-foreground/50 tracking-tight">
    jazz — sveltekit CSR / SSR
  </h1>
  <div class="grid grid-cols-3 gap-6">
    <section class="border border-foreground/10 rounded-lg p-5">
      <span class="text-xs font-mono text-foreground/40 uppercase tracking-widest mb-4 block">
        Client-side (Svelte)
      </span>
      <ClientChip />
      {@render columnNote(
        "This column is a fully client-rendered to-do list. Adding items works locally, and they sync to the server in the background.",
      )}
      <TodoPanel />
    </section>
    <section class="border border-foreground/10 rounded-lg p-5">
      <span class="text-xs font-mono text-foreground/40 uppercase tracking-widest mb-4 block">
        Server-side (load)
      </span>
      <Chip renderedOn="server" />
      {@render columnNote(
        "This column is fully server-rendered. Adding items runs a SvelteKit form action; on success the page load re-runs, so the list re-renders with the new item. This is not a live subscription — adding an item in another column won't update this list until you reload.",
      )}
      <ServerTodo todos={data.serverTodos} />
    </section>
    <section class="border border-foreground/10 rounded-lg p-5">
      <span class="text-xs font-mono text-foreground/40 uppercase tracking-widest mb-4 block">
        Server prefetch + client hydrate
      </span>
      <RenderChip />
      {@render columnNote(
        "This column first renders on the server, so the page already has its items on load. Then it hydrates on the client and further updates sync in real time. Add items here and watch this column and the client column update live.",
      )}
      <TodoPanel snapshot={data.snapshot} />
    </section>
  </div>
</main>
