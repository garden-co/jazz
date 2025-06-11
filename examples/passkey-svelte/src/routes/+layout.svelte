<script lang="ts">
  import { JazzProvider, PasskeyAuthBasicUI } from 'jazz-svelte';
  import { apiKey } from '../apiKey';
  import { Account } from 'jazz-tools';

  let { children } = $props();
</script>

<JazzProvider
  AccountSchema={Account}
  sync={{
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`
  }}
>
  <PasskeyAuthBasicUI appName="minimal-svelte-auth-passkey">
    {@render children?.()}
  </PasskeyAuthBasicUI>
</JazzProvider>

<style>
  :global(html, body) {
    margin: 0;
  }
</style>
