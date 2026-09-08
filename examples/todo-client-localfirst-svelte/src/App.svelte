<script lang="ts">
  import { onMount } from 'svelte';
  import { JazzSvelteClientProvider, createJazzClient, type JazzClient } from 'jazz-tools/svelte';
  import type { DbConfig } from 'jazz-tools';
  import { Toaster } from 'svelte-sonner';
  import { prepareAccountConfig } from './account.js';
  import TodoList from './TodoList.svelte';

  interface Props { config?: Partial<DbConfig>; }
  let { config: configOverrides = {} }: Props = $props();
  let client = $state<JazzClient>();
  let error = $state<string>();

  // #region context-setup-svelte
  onMount(() => {
    let cancelled = false;
    let active: JazzClient | undefined;
    void (async () => {
      const config = await prepareAccountConfig(configOverrides);
      if (cancelled) return;
      const opened = await createJazzClient(config);
      if (cancelled) { await opened.shutdown(); return; }
      active = opened;
      client = opened;
    })().catch((reason) => {
      if (!cancelled) error = reason instanceof Error ? reason.message : String(reason);
    });
    return () => {
      cancelled = true;
      client = undefined;
      void active?.shutdown().catch(console.error);
    };
  });
  // #endregion context-setup-svelte
</script>

{#if error}
  <p role="alert">{error}</p>
{:else if client}
  <JazzSvelteClientProvider {client}>
    {#snippet children()}
      <h1>Todos</h1>
      <TodoList />
      <Toaster />
    {/snippet}
  </JazzSvelteClientProvider>
{:else}
  <p>Loading...</p>
{/if}
