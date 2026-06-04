<!--
Test fixture: reads a query through QuerySubscription the same way a real
component would (instantiated in the script block). Used to prove the rows
are present in the server-rendered HTML, before any $effect runs.
-->
<script lang="ts">
  import type { QueryBuilder } from "../../runtime/db.js";
  import { QuerySubscription } from "../use-all.svelte.js";

  let { query }: { query: QueryBuilder<{ id: string; title: string }> } = $props();

  const todos = new QuerySubscription(() => query);
</script>

<ul>
  {#each todos.current ?? [] as todo}
    <li>{todo.title}</li>
  {/each}
</ul>
