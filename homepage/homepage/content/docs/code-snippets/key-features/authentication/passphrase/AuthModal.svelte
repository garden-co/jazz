<script lang="ts">
  import { usePassphraseAuth } from "jazz-tools/svelte";
  import { wordlist } from "./wordlist";
  let loginPassphrase = $state('');
  const auth = usePassphraseAuth({
    // Must be inside the JazzProvider!
    wordlist: wordlist,
  });
  const handleSignUp = async () => {
    await auth.signUp();
  };

  const handleLogIn = async () => {
    await auth.logIn(loginPassphrase);
  };
</script>

{#if auth.state === "signedIn"}
  <div>You are already signed in</div>
{:else}
<div>
  <label>
    Your current passphrase
    <textarea readOnly value={auth.passphrase} rows={5}></textarea>
  </label>
  <button onclick={handleSignUp}>I have stored my passphrase</button>
  <label>
    Log in with your passphrase
    <textarea
      bind:value={loginPassphrase}
      placeholder="Enter your passphrase"
      rows={5}
      required
    ></textarea>
  </label>
  <button onclick={handleLogIn}>Log in</button>
</div>
{/if}
