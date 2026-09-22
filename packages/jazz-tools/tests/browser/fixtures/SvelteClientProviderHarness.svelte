<script lang="ts">
  import type { JazzClient } from "../../../src/svelte/create-jazz-client.js";
  import JazzSvelteClientProvider from "../../../src/svelte/JazzSvelteClientProvider.svelte";

  interface Props {
    client: JazzClient | Promise<JazzClient>;
  }

  let { client }: Props = $props();
</script>

<JazzSvelteClientProvider {client} autoAttachDevTools={false}>
  {#snippet children({ db })}
    <p data-client-provider-state="ready">{db.getAuthState().session?.authMode}</p>
  {/snippet}
  {#snippet fallback()}
    <p data-client-provider-state="loading">loading</p>
  {/snippet}
</JazzSvelteClientProvider>
