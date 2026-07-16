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
  import TodoForm from "./TodoForm.svelte";
  import TodoList from "./TodoList.svelte";

  // Pass a snapshot to seed the first render (the prefetch column does); leave it
  // off and the panel just waits for the live client (the client-only column).
  // The panel itself cares about nothing else.
  let { snapshot }: { snapshot?: DehydratedSnapshot } = $props();

  let client = $state<Promise<JazzClient> | undefined>(undefined);
  const appId = env.PUBLIC_JAZZ_APP_ID!;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL!;

  onMount(async () => {
    const secret = await BrowserAuthSecretStore.getOrCreateSecret({ appId });
    client = createJazzClient({ appId, serverUrl, secret });
  });
</script>

<JazzSvelteProvider {client} ssr={!!snapshot} {appId}>
  {#snippet children()}
    <TodoForm />
    <TodoList {snapshot} />
  {/snippet}
  {#snippet fallback()}
    <p class="mt-4 text-sm text-foreground/30 italic">Connecting…</p>
  {/snippet}
</JazzSvelteProvider>
