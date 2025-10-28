<script lang="ts">
  import { JazzSvelteProvider } from 'jazz-tools/svelte';
  import 'jazz-tools/inspector/register-custom-element';
  import { PasskeyAuthBasicUI } from 'jazz-tools/svelte';
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

<JazzSvelteProvider
  AccountSchema={FileShareAccount}
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  }}
>
  <jazz-inspector></jazz-inspector>
  <PasskeyAuthBasicUI appName="File Share">
    <div class="min-h-screen bg-gray-100">
      {@render children()}
    </div>
  </PasskeyAuthBasicUI>
</JazzSvelteProvider>
