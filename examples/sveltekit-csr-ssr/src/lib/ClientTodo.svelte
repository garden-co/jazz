<script lang="ts">
  import { onMount } from "svelte";
  import { env } from "$env/dynamic/public";
  import {
    BrowserAuthSecretStore,
    createJazzClient,
    JazzSvelteProvider,
    type JazzClient,
  } from "jazz-tools/svelte";
  import ClientTodoForm from "./ClientTodoForm.svelte";
  import ClientTodoList from "./ClientTodoList.svelte";

  const appId = env.PUBLIC_JAZZ_APP_ID!;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL!;

  let client = $state<Promise<JazzClient> | null>(null);

  onMount(async () => {
    const secret = await BrowserAuthSecretStore.getOrCreateSecret({ appId });
    client = createJazzClient({ appId, serverUrl, secret });
  });
</script>

{#if client}
  <JazzSvelteProvider {client}>
    {#snippet children()}
      <ClientTodoForm />
      <ClientTodoList />
    {/snippet}
    {#snippet fallback()}
      <p class="mt-4 text-sm text-foreground/30 italic">Connecting…</p>
    {/snippet}
  </JazzSvelteProvider>
{/if}
