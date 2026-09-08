<script lang="ts">
  import { onMount, tick, untrack, type Snippet } from "svelte";
  import { writable } from "svelte/store";
  import { createJazzApp, type JazzAppConfig } from "../session/create-jazz-app.js";
  import type { JazzApp, JazzAppSnapshot, JazzAuth } from "../session/app.js";
  import type { JazzClient } from "./create-jazz-client.js";
  import JazzSvelteClientProvider from "./JazzSvelteClientProvider.svelte";
  import { setJazzAuth } from "./auth-state.js";

  type Props = JazzAppConfig & {
    children: Snippet;
    signedOut?: Snippet;
    loading?: Snippet;
    error?: Snippet<[Error, () => Promise<void>]>;
    autoAttachDevTools?: boolean;
  };
  let { children, signedOut, loading, error, autoAttachDevTools = true, auth, ...config }: Props = $props();
  // Configuration belongs to one mounted provider. Key the provider to replace it.
  const initialConfig = untrack(() => config);
  const snapshotStore = writable<JazzAppSnapshot<JazzClient>>({ status: "starting" });
  let owner: JazzApp<JazzClient> | undefined;
  const retry = () => owner?.retry() ?? Promise.resolve();
  setJazzAuth({ subscribe: snapshotStore.subscribe, retry, logout: () => owner?.logout() ?? Promise.resolve() });
  onMount(() => {
    const app = createJazzApp({ ...initialConfig, initial: initialConfig.initial ?? (auth ? undefined : "local-first"), auth });
    owner = app;
    const lease = app.attachConsumer();
    const update = () => {
      const observed = app.getSnapshot();
      snapshotStore.set(observed);
      if (!observed.client) void tick().then(() => lease.acknowledge(observed));
    };
    const unsubscribe = app.subscribe(update);
    update();
    return () => {
      owner = undefined;
      unsubscribe();
      lease.release();
      void app.dispose().catch(console.error);
    };
  });
  $effect(() => { const next = auth as JazzAuth | undefined; owner?.updateAuth(next); });
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
    <JazzSvelteClientProvider client={$snapshotStore.client} {autoAttachDevTools}>
      {#snippet children()}{@render children()}{/snippet}
      {#snippet fallback()}{@render pending()}{/snippet}
    </JazzSvelteClientProvider>
  {/key}
{:else}
  {@render pending()}
{/if}
