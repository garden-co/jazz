<!--
Test fixture: a snapshot-seeded provider with NO client, wrapped in an error
boundary. If the provider tries to resolve the (absent) client it throws
asynchronously; the boundary captures that via `onError` so a test can assert it
never happens.
-->
<script lang="ts">
  import type { DehydratedSnapshot } from "../../backend/ssr.js";
  import type { QueryBuilder } from "../../runtime/db.js";
  import JazzSvelteProvider from "../JazzSvelteProvider.svelte";
  import SsrTodoList from "./SsrTodoList.svelte";

  let {
    snapshot,
    query,
    onError,
  }: {
    snapshot: DehydratedSnapshot;
    query: QueryBuilder<{ id: string; title: string }>;
    onError: (error: unknown) => void;
  } = $props();
</script>

<svelte:boundary onerror={onError}>
  <JazzSvelteProvider {snapshot}>
    {#snippet children()}
      <SsrTodoList {query} />
    {/snippet}
  </JazzSvelteProvider>

  {#snippet failed()}
    <p>boundary-failed</p>
  {/snippet}
</svelte:boundary>
