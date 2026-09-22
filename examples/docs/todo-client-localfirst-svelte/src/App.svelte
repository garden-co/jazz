<!-- #region provider-svelte -->
<script lang="ts">
  import type { DbConfig } from "jazz-tools";
  import { JazzSvelteProvider } from 'jazz-tools/svelte';
  import TodoList from './TodoList.svelte';

  let { config }: { config: DbConfig } = $props();
</script>

<JazzSvelteProvider {config}>
  {#snippet children()}
    <h1>Todos</h1>
    <TodoList />
  {/snippet}
  {#snippet fallback()}
    <p>Loading...</p>
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion provider-svelte -->
