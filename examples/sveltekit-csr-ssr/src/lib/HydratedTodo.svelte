<script lang="ts">
  import { onMount } from "svelte";
  import { env } from "$env/dynamic/public";
  import {
    BrowserAuthSecretStore,
    createJazzClient,
    JazzSvelteProvider,
    type DehydratedSnapshot,
    type JazzClient,
  } from "jazz-tools/svelte";
  import { app } from "$lib/schema";
  import ClientTodoForm from "./ClientTodoForm.svelte";
  import ClientTodoList from "./ClientTodoList.svelte";

  let { snapshot }: { snapshot: DehydratedSnapshot } = $props();

  // No client on the server; the snapshot drives the SSR render and first paint.
  // The browser creates one in onMount, which then takes over.
  let client = $state<Promise<JazzClient> | undefined>(undefined);

  const appId = env.PUBLIC_JAZZ_APP_ID!;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL!;

  onMount(async () => {
    const secret = await BrowserAuthSecretStore.getOrCreateSecret({ appId });
    client = createJazzClient({ appId, serverUrl, secret });
  });
</script>

<JazzSvelteProvider {client} {snapshot} schema={app}>
  {#snippet children()}
    <ClientTodoForm />
    <ClientTodoList />
  {/snippet}
</JazzSvelteProvider>
