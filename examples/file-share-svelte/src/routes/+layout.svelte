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
  import {apiKey} from '../apiKey';

  let { children } = $props();
</script>

<svelte:head>
  <title>File Share</title>
</svelte:head>

<Toaster richColors />

<JazzProvider
  AccountSchema={FileShareAccount}
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  }}
>
  <PasskeyAuthBasicUI appName="File Share">
    <div class="min-h-screen bg-gray-100">
      {@render children()}
    </div>
  </PasskeyAuthBasicUI>
</JazzProvider>
