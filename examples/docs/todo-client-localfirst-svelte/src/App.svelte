<!-- #region provider-svelte -->
<script lang="ts">
  import { JazzSvelteProvider } from 'jazz-tools/svelte';
  import TodoList from './TodoList.svelte';

  const config = {
    appId: '<your-app-id>',
  };
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
