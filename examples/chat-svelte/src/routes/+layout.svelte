<script lang="ts">
  import '../app.css';
  import { JazzProvider } from 'jazz-svelte';
  import 'jazz-inspector-element';
  import { page } from '$app/state';
  import { apiKey } from '../apiKey';
  import { getRandomUsername } from '$lib/utils';
  let { children } = $props();
  const { url } = $derived(page);
  const defaultProfileName = $derived(url.searchParams.get('user') ?? getRandomUsername());
  const peer = $derived(url.searchParams.get('peer') ?? `wss://cloud.jazz.tools`) as
    | `wss://${string}`
    | `ws://${string}`;
</script>

<svelte:head>
  <title>Jazz Chat Example</title>
</svelte:head>

<div class="h-full bg-white text-stone-700 dark:text-stone-400 dark:bg-stone-925">
  <JazzProvider
    sync={{
      peer: `${peer}/?key=${apiKey}`
    }}
    {defaultProfileName}
  >
    {@render children?.()}
  </JazzProvider>
  <jazz-inspector></jazz-inspector>
</div>

<style>
  :global(html, body) {
    margin: 0;
  }
</style>
