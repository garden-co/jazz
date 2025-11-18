<script lang="ts">
  import { usePasskeyAuth } from 'jazz-tools/svelte';
  const { children } = $props();
  const auth = usePasskeyAuth({ appName: 'JazzFest' });
  let name = $state('');
</script>

{#if auth.state === "signedIn"}
  {@render children?.()}
{:else}
  <div>
    <button onclick={() => auth.current.logIn()}>Log in</button>
    <input type="text" bind:value={name} />
    <button onclick={() => auth.current.signUp(name)}>Sign up</button>
  </div>
{/if}
