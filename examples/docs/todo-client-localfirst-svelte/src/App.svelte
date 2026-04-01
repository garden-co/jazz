<!-- #region provider-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
  import TodoList from './TodoList.svelte';

  const client = createJazzClient({
    appId: 'my-todo-app',
  });
</script>

<JazzSvelteProvider {client}>
  {#snippet children()}
    <h1>Todos</h1>
    <TodoList />
  {/snippet}
  {#snippet fallback()}
    <p>Loading...</p>
  {/snippet}
</JazzSvelteProvider>
<!-- #endregion provider-svelte -->
