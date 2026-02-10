<script lang="ts">
  import { JazzSvelteProvider } from 'jazz-tools/svelte';
  import { Toaster } from 'svelte-sonner';
  import { getRandomUsername, inIframe } from '@/lib/utils';
  import { ChatAccount } from '@/lib/schema';
  import { apiKey } from './apiKey';
  import NavBar from '@/components/navbar/NavBar.svelte';
  import Router from '@/components/Router.svelte';

  const url = typeof window !== 'undefined' ? new URL(window.location.href) : null;
  const defaultProfileName = url?.searchParams.get('user') ?? getRandomUsername();
</script>

<JazzSvelteProvider
  authSecretStorageKey="examples/chat-svelte"
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`
  }}
  {defaultProfileName}
  AccountSchema={ChatAccount}
>
  <main class="flex flex-col h-screen bg-muted text-muted-foreground">
    <NavBar />
    <div class="flex-1 min-h-0">
      <Router />
    </div>
  </main>
  <Toaster richColors />
  {#if !inIframe}
    <jazz-inspector></jazz-inspector>
  {/if}
</JazzSvelteProvider>
