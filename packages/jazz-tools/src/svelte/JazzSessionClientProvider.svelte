<script lang="ts">
  import { onMount, tick, untrack, type Snippet } from 'svelte';
  import { attachJazzSessionConsumer } from '../session/consumer.js';
  import type { JazzSession } from '../session/state.js';
  import type { JazzClient } from './create-jazz-client.js';
  import JazzSvelteClientProvider from './JazzSvelteClientProvider.svelte';
  import { setJazzSession } from './session-state.js';

  interface Props {
    session: JazzSession<JazzClient>;
    children: Snippet;
    fallback?: Snippet;
    autoAttachDevTools?: boolean;
  }
  let { session, children, fallback, autoAttachDevTools = true }: Props = $props();
  // A provider owns one session; replace/key the provider to change owners.
  const owner = untrack(() => session);
  setJazzSession(owner);
  let snapshot = $state(owner.getSnapshot());
  onMount(() => {
    const consumer = attachJazzSessionConsumer(owner);
    const update = () => {
      const observed = owner.getSnapshot();
      snapshot = observed;
      if (!observed.client) void tick().then(() => consumer.acknowledge(observed));
    };
    const unsubscribe = owner.subscribe(update);
    update();
    return () => { unsubscribe(); consumer.release(); };
  });
</script>

{#if snapshot.client}
  {#key snapshot.client}
    <JazzSvelteClientProvider client={snapshot.client} {autoAttachDevTools}>
      {#snippet children()}{@render children()}{/snippet}
      {#snippet fallback()}{@render fallback?.()}{/snippet}
    </JazzSvelteClientProvider>
  {/key}
{:else}
  {@render fallback?.()}
{/if}
