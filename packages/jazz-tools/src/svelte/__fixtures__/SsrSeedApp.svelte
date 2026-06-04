<!--
Test fixture: a snapshot-seeded provider wrapping a query reader, so an SSR
render exercises the synchronous seed path end-to-end (no live client, no
$effect).
-->
<script lang="ts">
  import type { DehydratedSnapshot } from "../../backend/ssr.js";
  import type { QueryBuilder } from "../../runtime/db.js";
  import type { JazzClient } from "../create-jazz-client.js";
  import JazzSvelteProvider from "../JazzSvelteProvider.svelte";
  import SsrTodoList from "./SsrTodoList.svelte";

  let {
    client,
    snapshot,
    query,
  }: {
    client?: JazzClient | Promise<JazzClient>;
    snapshot: DehydratedSnapshot;
    query: QueryBuilder<{ id: string; title: string }>;
  } = $props();
</script>

<JazzSvelteProvider {client} {snapshot}>
  {#snippet children()}
    <SsrTodoList {query} />
  {/snippet}
</JazzSvelteProvider>
