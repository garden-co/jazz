<script lang="ts">
  // [!code ++:3]
  import { usePasskeyAuth, usePassphraseAuth } from 'jazz-tools/svelte';
  import { wordlist } from './wordlist';
  const { children } = $props();
  const passphraseAuth = usePassphraseAuth({ wordlist });
  const auth = usePasskeyAuth({ appName: 'JazzFest' });
  let name = $state('');
</script>

{#if auth.state === "signedIn"}
  {@render children?.()}
  <!-- [!code ++:5] -->
  <textarea
    readonly
    value={passphraseAuth.passphrase}
    rows={5}
  ></textarea>
{:else}
  <div>
    <button onclick={() => auth.current.logIn()}>Log in</button>
    <input type="text" bind:value={name} />
    <button onclick={() => auth.current.signUp(name)}>Sign up</button>
  </div>
{/if}
