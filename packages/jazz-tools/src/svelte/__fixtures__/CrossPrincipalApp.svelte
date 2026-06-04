<!--
Test fixture: a snapshot-seeded provider given a live client, wrapped in an error
boundary so a test can assert what the provider throws at the live-swap — e.g. a
snapshot scoped to one principal reaching a client authenticated as another.
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
    onError,
  }: {
    client: JazzClient | Promise<JazzClient>;
    snapshot: DehydratedSnapshot;
    query: QueryBuilder<{ id: string; title: string }>;
    onError: (error: unknown) => void;
  } = $props();
</script>

<svelte:boundary onerror={onError}>
  <JazzSvelteProvider {client} {snapshot}>
    {#snippet children()}
      <SsrTodoList {query} />
    {/snippet}
  </JazzSvelteProvider>

  {#snippet failed()}
    <p>boundary-failed</p>
  {/snippet}
</svelte:boundary>
