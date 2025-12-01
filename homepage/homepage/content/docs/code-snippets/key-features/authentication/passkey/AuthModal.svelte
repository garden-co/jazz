<script lang="ts">
  import { usePasskeyAuth } from 'jazz-tools/svelte';
  const auth = usePasskeyAuth({ appName: 'My super-cool web app' });
  let username = $state('');

  async function handleSignUp(e: Event) {
    await auth.current.signUp(username);
  }

  async function handleLogIn(e: Event) {
    await auth.current.logIn();
  }
</script>

<div>
  <button onclick={handleLogIn}>Log in</button>
  <input type="text" bind:value={username} />
  <button onclick={handleSignUp}>Sign up</button>
</div>
