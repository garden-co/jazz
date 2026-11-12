<!-- #region provider-svelte -->
<script lang="ts">
  import { JazzProvider, type JazzClientConfig } from 'jazz-tools/svelte';
  import TodoList from './TodoList.svelte';

  let { config }: { config: JazzClientConfig } = $props();
</script>

<JazzProvider {config}>
  {#snippet children()}
    <h1>Todos</h1>
    <TodoList />
  {/snippet}
  {#snippet loading()}
    <p>Loading...</p>
  {/snippet}
</JazzProvider>
<!-- #endregion provider-svelte -->
