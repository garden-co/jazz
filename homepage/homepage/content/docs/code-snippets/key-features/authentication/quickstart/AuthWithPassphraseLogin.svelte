<script lang="ts">
  import { usePasskeyAuth, usePassphraseAuth } from 'jazz-tools/svelte';
  import { wordlist } from './wordlist';
  const { children } = $props();
  const passphraseAuth = usePassphraseAuth({ wordlist });
  const auth = usePasskeyAuth({ appName: 'JazzFest' });
  let name = $state('');
  // [!code ++:1]
  let passphrase = $state('');
</script>

{#if auth.state === "signedIn"}
  {@render children?.()}
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
    <!-- [!code ++:7] -->
    <textarea
      bind:value={passphrase}
      rows={5}
    ></textarea>
    <button onclick={() => passphraseAuth.logIn(passphrase)}>
      Sign In with Passphrase
    </button>
  </div>
{/if}
