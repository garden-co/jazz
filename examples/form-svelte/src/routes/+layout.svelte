<script lang="ts">
  import { JazzSvelteProvider } from 'jazz-tools/svelte';
  import 'jazz-tools/inspector/register-custom-element';
  import { PasskeyAuthBasicUI } from 'jazz-tools/svelte';
  import { apiKey } from '$lib/apiKey';
  import { JazzAccount } from '$lib/schema';
  import '../app.css';
  
  let { children } = $props();
</script>

<svelte:head>
  <title>Form Example</title>
</svelte:head>

<JazzSvelteProvider
  AccountSchema={JazzAccount}
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
  }}
>
  <jazz-inspector></jazz-inspector>
  <PasskeyAuthBasicUI appName="Form Example">
    <div class="min-h-screen bg-gray-100">
      {@render children()}
    </div>
  </PasskeyAuthBasicUI>
</JazzSvelteProvider> 
