<script lang="ts">
  import { onMount } from 'svelte';
  import { JazzSvelteProvider } from 'jazz-tools/svelte';
  import type { DbConfig } from 'jazz-tools';
  import { Toaster } from 'svelte-sonner';
  import { prepareAccountConfig } from './account.js';
  import TodoList from './TodoList.svelte';

  interface Props { config?: Partial<DbConfig>; }
  let { config: configOverrides = {} }: Props = $props();
  let config = $state<DbConfig>();
  let error = $state<string>();

  // #region context-setup-svelte
  onMount(() => {
    let cancelled = false;
    void prepareAccountConfig(configOverrides)
      .then((prepared) => {
        if (!cancelled) config = prepared;
      })
      .catch((reason) => {
        if (!cancelled) error = reason instanceof Error ? reason.message : String(reason);
      });
    return () => {
      cancelled = true;
    };
  });
  // #endregion context-setup-svelte
</script>

{#if error}
  <p role="alert">{error}</p>
{:else if config}
  <JazzSvelteProvider {config}>
    {#snippet children()}
      <h1>Todos</h1>
      <TodoList />
      <Toaster />
    {/snippet}
    {#snippet fallback()}
      <p>Loading...</p>
    {/snippet}
  </JazzSvelteProvider>
{:else}
  <p>Loading...</p>
{/if}
