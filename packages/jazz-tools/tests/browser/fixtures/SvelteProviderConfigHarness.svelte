<script lang="ts">
  import type { DbConfig } from "../../../src/runtime/db.js";
  import JazzSvelteProvider from "../../../src/svelte/JazzSvelteProvider.svelte";

  interface Props {
    initialConfig: DbConfig;
    replacementConfig: DbConfig;
  }

  let { initialConfig, replacementConfig }: Props = $props();
  let useReplacement = $state(false);
  let config = $derived(useReplacement ? replacementConfig : initialConfig);

  export function useReplacementConfig(): void {
    useReplacement = true;
  }
</script>

<JazzSvelteProvider {config} autoAttachDevTools={false}>
  {#snippet children({ db })}
    <p data-provider-state="ready">{db.getAuthState().session?.authMode}</p>
  {/snippet}
  {#snippet fallback()}
    <p data-provider-state="loading">loading</p>
  {/snippet}
</JazzSvelteProvider>
