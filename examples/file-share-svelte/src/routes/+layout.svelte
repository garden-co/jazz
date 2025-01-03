<script lang="ts" module>
  declare module 'jazz-svelte' {
    interface Register {
      Account: FileShareAccount;
    }
  }
</script>

<script lang="ts">
  import { JazzProvider } from 'jazz-svelte';
  import { PasskeyAuthBasicUI, usePasskeyAuth } from 'jazz-svelte';
  import { Toaster } from 'svelte-sonner';
  import '../app.css';
  import { FileShareAccount } from '$lib/schema';

  let { children } = $props();
  const auth = usePasskeyAuth({
    appName: 'File Share'
  });
</script>

<svelte:head>
  <title>File Share</title>
</svelte:head>

<Toaster richColors />

{#if auth.state.state === 'ready'}
  <div class="fixed inset-0 flex items-center justify-center bg-gray-50/80">
    <div class="rounded-lg bg-white p-8 shadow-lg">
      <PasskeyAuthBasicUI state={auth.state} />
    </div>
  </div>
{/if}
{#if auth.current}
  <JazzProvider
    AccountSchema={FileShareAccount}
    auth={auth.current}
    peer="wss://cloud.jazz.tools/?key=file-share-svelte@garden.co"
  >
    <div class="min-h-screen bg-gray-100">
      {@render children()}
    </div>
  </JazzProvider>
{/if}
