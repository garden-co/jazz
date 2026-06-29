<!-- #region provider-svelte -->
<script lang="ts">
  import { createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
  import TodoList from './TodoList.svelte';

  const client = createJazzClient({
    appId: 'my-app', // Register for an app ID free at v2.dashboard.jazz.tools
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
