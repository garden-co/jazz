<script lang="ts">
  import type { AccountDbConfig as DbConfig } from "../../../src/accounts/context.js";
  import JazzSvelteProvider from "../../../src/svelte/JazzSvelteProvider.svelte";

  interface Props {
    initialConfig: DbConfig;
    replacementConfig: DbConfig;
  }

  let { initialConfig, replacementConfig }: Props = $props();
  let useReplacement = $state(false);
  let effectiveConfig = $derived(useReplacement ? replacementConfig : initialConfig);

  export function useReplacementConfig(): void {
    useReplacement = true;
  }
</script>

<JazzSvelteProvider config={effectiveConfig} autoAttachDevTools={false}>
  {#snippet children({ db })}
    <p data-provider-account>{db.getAuthState().session?.user.account}</p>
    <p data-provider-state="ready">{db.getAuthState().session?.authMode}</p>
    <p data-provider-user={db.getAuthState().session?.user ?? ""}>
      {db.getAuthState().session?.user}
    </p>
  {/snippet}
  {#snippet fallback()}
    <p data-provider-state="loading">loading</p>
  {/snippet}
</JazzSvelteProvider>
