<script lang="ts">
  import { onMount, tick, untrack, type Snippet } from "svelte";
  import { writable } from "svelte/store";
  import { openBrowserSessionCore, type JazzClientConfig } from "../session/create-jazz-session.js";
  import {
    attachJazzSessionConsumer,
    ownJazzSession,
    startJazzSession,
    type JazzSession,
  } from "../session/jazz-session.js";
  import type { JazzClient } from "./create-jazz-client.js";
  import JazzSvelteClientProvider from "./JazzSvelteClientProvider.svelte";
  import { setJazzSession } from "./session-state.js";
  import { refuseAutoAttachDevTools, type NoAutoAttachDevTools } from "../runtime/removed-provider-props.js";
  import { assertNoClassicProviderProps } from "../classic-api.js";

  type Props = NoAutoAttachDevTools & {
    children: Snippet;
    signedOut?: Snippet;
    loading?: Snippet;
    error?: Snippet<[Error, () => Promise<void>]>;
  } & (
      | { config: JazzClientConfig; session?: never }
      | { session: JazzSession<JazzClient>; config?: never }
    );
  let { children, signedOut, loading, error, config, session: external, ...rest }: Props = $props();
  // Re-checked whenever the props change: Classic configuration arriving
  // later is refused too.
  $effect.pre(() => {
    refuseAutoAttachDevTools("JazzProvider", rest);
    assertNoClassicProviderProps("JazzProvider", rest);
  });
  refuseAutoAttachDevTools("JazzProvider", rest);
  assertNoClassicProviderProps("JazzProvider", rest);
  // One provider, one session: key the provider to replace its configuration.
  const owned = untrack(() => !external);
  const session = untrack(() => {
    if (!config === !external)
      throw new Error(
        "<JazzProvider> takes either `config` (its session's configuration, e.g. config={{ serverUrl: \"/jazz\" }}) or a `session` from createJazzClient.",
      );
    if (config && "account" in config)
      throw new Error(
        "<JazzProvider config> is the session's configuration and takes no `account`: the session signs in itself (createDeviceAccount(), or your backend's user). For a client you opened yourself, use <JazzSvelteClientProvider client>.",
      );
    return external ?? ownJazzSession({ ...config! }, openBrowserSessionCore, { start: false });
  });
  setJazzSession(session);
  const snapshotStore = writable(session.getSnapshot());
  const retry = () => session.retry().catch(() => {});
  onMount(() => {
    const lease = attachJazzSessionConsumer(session);
    const update = () => {
      const observed = session.getSnapshot();
      snapshotStore.set(observed);
      if (observed.status !== "ready") void tick().then(() => lease.acknowledge(observed));
    };
    const unsubscribe = session.subscribe(update);
    update();
    if (owned) void startJazzSession(session as JazzSession<unknown>).catch(() => {});
    return () => {
      unsubscribe();
      lease.release();
      if (owned) void session.close().catch(console.error);
    };
  });
</script>

{#snippet pending()}
  {#if $snapshotStore.status === "error" && $snapshotStore.error}
    {#if error}{@render error($snapshotStore.error, retry)}{:else}
      <p role="alert">{$snapshotStore.error.message}</p>
      <button onclick={() => void retry()}>Retry</button>
    {/if}
  {:else if $snapshotStore.status === "signed-out"}
    {@render signedOut?.()}
  {:else if loading}
    {@render loading()}
  {:else}
    <p role="status">Loading...</p>
  {/if}
{/snippet}
{#if $snapshotStore.status === "ready" && $snapshotStore.client}
  {#key $snapshotStore.client}
    <JazzSvelteClientProvider client={$snapshotStore.client}>
      {#snippet children()}{@render children()}{/snippet}
      {#snippet fallback()}{@render pending()}{/snippet}
    </JazzSvelteClientProvider>
  {/key}
{:else}
  {@render pending()}
{/if}
