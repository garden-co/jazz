<script lang="ts">
  import { onMount, tick, untrack, type Snippet } from "svelte";
  import { writable } from "svelte/store";
  import { createJazzApp, type JazzAppConfig } from "../session/create-jazz-app.js";
  import JazzSvelteClientProvider from "./JazzSvelteClientProvider.svelte";
  import { setJazzAuth } from "./auth-state.js";
  import { authForUser } from "../session/app.js";

  type Props = JazzAppConfig & {
    children: Snippet;
    signedOut?: Snippet;
    loading?: Snippet;
    error?: Snippet<[Error, () => Promise<void>]>;
    autoAttachDevTools?: boolean;
  };
  // `user` stays in `config` so its presence is visible: `user={undefined}` (loading) is not no prop.
  let { children, signedOut, loading, error, autoAttachDevTools = true, auth, ...config }: Props = $props();
  const currentAuth = () => authForUser("user" in config, config.user, auth);
  // Configuration belongs to one mounted provider. Key the provider to replace it.
  const app = untrack(() => createJazzApp({ ...config, auth }, { start: false }));
  const snapshotStore = writable(app.getSnapshot());
  const retry = () => app.retry().catch(() => {});
  setJazzAuth({ subscribe: snapshotStore.subscribe, retry, logout: () => app.logout().catch(() => {}), sessionActions: app.sessionActions });
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
  $effect.pre(() => { app.updateAuth(currentAuth()); });
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
