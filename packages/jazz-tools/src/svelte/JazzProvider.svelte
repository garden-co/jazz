<script lang="ts">
  import { onMount, tick, untrack, type Snippet } from "svelte";
  import { writable } from "svelte/store";
  import { createJazzApp, type JazzAppConfig } from "../session/create-jazz-app.js";
  import JazzSvelteClientProvider from "./JazzSvelteClientProvider.svelte";
  import { setJazzAuth } from "./auth-state.js";
  import { refuseAutoAttachDevTools, type NoAutoAttachDevTools } from "../runtime/removed-provider-props.js";

  type Props = JazzAppConfig & NoAutoAttachDevTools & {
    children: Snippet;
    signedOut?: Snippet;
    loading?: Snippet;
    error?: Snippet<[Error, () => Promise<void>]>;
  };
  let { children, signedOut, loading, error, ...config }: Props = $props();
  refuseAutoAttachDevTools("JazzProvider", config);
  // Configuration belongs to one mounted provider. Key the provider to replace it.
  const app = untrack(() => createJazzApp({ ...config }, { start: false }));
  const snapshotStore = writable(app.getSnapshot());
  const retry = () => app.retry().catch(() => {});
  setJazzAuth({ subscribe: snapshotStore.subscribe, retry, reauthenticate: () => app.reauthenticate().catch(() => {}), logout: () => app.logout().catch(() => {}), sessionActions: app.sessionActions });
  onMount(() => {
    const lease = app.attachConsumer();
    const update = () => {
      const observed = app.getSnapshot();
      snapshotStore.set(observed);
      if (!observed.client) void tick().then(() => lease.acknowledge(observed));
    };
    const unsubscribe = app.subscribe(update);
    update();
    void app.start().catch(() => {});
    return () => {
      unsubscribe();
      lease.release();
      void app.dispose().catch(console.error);
    };
  });
</script>

{#snippet pending()}
  {#if $snapshotStore.status === "error" && $snapshotStore.error}
    {#if error}{@render error($snapshotStore.error, retry)}{:else}
      <p role="alert">{$snapshotStore.error.message}</p>
      <button onclick={() => void retry().catch(() => {})}>Retry</button>
    {/if}
  {:else if $snapshotStore.status === "signed-out"}
    {@render signedOut?.()}
  {:else if loading}
    {@render loading()}
  {:else}
    <p role="status">Loading...</p>
  {/if}
{/snippet}
{#if $snapshotStore.client}
  {#key $snapshotStore.client}
    <JazzSvelteClientProvider client={$snapshotStore.client}>
      {#snippet children()}{@render children()}{/snippet}
      {#snippet fallback()}{@render pending()}{/snippet}
    </JazzSvelteClientProvider>
  {/key}
{:else}
  {@render pending()}
{/if}
