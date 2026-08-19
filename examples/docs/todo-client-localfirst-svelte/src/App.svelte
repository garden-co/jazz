<!-- #region provider-svelte -->
<script lang="ts">
  import { LocalFirstAuth, createJazzClient, JazzSvelteProvider } from 'jazz-tools/svelte';
  import TodoList from './TodoList.svelte';

  const auth = new LocalFirstAuth();
  const client = $derived(
    !auth.isLoading && auth.secret
      ? createJazzClient({ appId: '<your-app-id>', secret: auth.secret })
      : null,
  );
</script>

{#if client}
  <JazzSvelteProvider {client}>
    {#snippet children()}
      <h1>Todos</h1>
      <TodoList />
    {/snippet}
    {#snippet fallback()}
      <p>Loading...</p>
    {/snippet}
  </JazzSvelteProvider>
{/if}
<!-- #endregion provider-svelte -->
