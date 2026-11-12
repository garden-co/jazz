<script lang="ts">
  import { untrack } from 'svelte';
  import { JazzProvider } from 'jazz-tools/svelte';
  import { Toaster } from 'svelte-sonner';
  import { sessionConfig, type SessionOverrides } from './account.js';
  import TodoList from './TodoList.svelte';

  interface Props { config?: SessionOverrides; }
  let { config: configOverrides = {} }: Props = $props();

  // #region context-setup-svelte
  // Captured at mount: the provider owns one session for its lifetime.
  const config = untrack(() => sessionConfig(configOverrides));
  // #endregion context-setup-svelte
</script>

<JazzProvider {config}>
  {#snippet children()}
    <h1>Todos</h1>
    <TodoList />
    <Toaster />
  {/snippet}
  {#snippet loading()}
    <p>Loading...</p>
  {/snippet}
</JazzProvider>
