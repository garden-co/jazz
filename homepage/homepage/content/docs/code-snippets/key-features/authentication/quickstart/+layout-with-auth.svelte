<script lang="ts">
  // [!code ++:2]
  import { JazzSvelteProvider } from "jazz-tools/svelte";
  import Auth from '$lib/components/Auth.svelte';
  import { JazzFestAccount } from "$lib/schema";
  // @ts-expect-error Will be there in a real app
  import { PUBLIC_JAZZ_API_KEY } from "$env/static/public";
  import { SyncConfig } from "jazz-tools";

  const apiKey = PUBLIC_JAZZ_API_KEY;
  let { children } = $props();
  const sync: SyncConfig = { peer: `wss://cloud.jazz.tools/?key=${apiKey}` };
</script>

<JazzSvelteProvider {sync} AccountSchema={JazzFestAccount}>
  <!-- [!code ++:3] -->
  <Auth>
    {@render children?.()}
  </Auth>
</JazzSvelteProvider>
