<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";

  type Mode = "signIn" | "signUp";

  let mode = $state<Mode>("signIn");
  let name = $state("");
  let email = $state("");
  let password = $state("");
  let error = $state<string | null>(null);

  const isSignUp = $derived(mode === "signUp");

  async function handleSubmit(e: SubmitEvent) {
    e.preventDefault();
    error = null;
    const res = isSignUp
      ? await authClient.signUp.email({ email, name, password })
      : await authClient.signIn.email({ email, password });
    if (res.error) {
      error = res.error.message ?? `${isSignUp ? "Sign-up" : "Sign-in"} failed`;
      return;
    }
    await goto("/dashboard");
  }

  function toggleMode() {
    mode = isSignUp ? "signIn" : "signUp";
    error = null;
  }
</script>

<main class="page-center">
  <img src="/jazz.svg" alt="Jazz" class="wordmark" />
  <div class="card">
    <h1>{isSignUp ? "Create account" : "Sign in"}</h1>
    <form onsubmit={handleSubmit}>
      {#if isSignUp}
        <div class="field">
          <label for="name">Name</label>
          <input id="name" type="text" bind:value={name} required />
        </div>
      {/if}
      <div class="field">
        <label for="email">Email</label>
        <input id="email" type="email" bind:value={email} required />
      </div>
      <div class="field">
        <label for="password">Password</label>
        <input id="password" type="password" bind:value={password} required />
      </div>
      {#if error}
        <p class="alert-error" role="alert">{error}</p>
      {/if}
      <button type="submit" class="btn-primary">
        {isSignUp ? "Create account" : "Sign in"}
      </button>
    </form>
    <p class="toggle">
      {isSignUp ? "Already have an account?" : "New here?"}
      <button type="button" class="link" onclick={toggleMode}>
        {isSignUp ? "Sign in" : "Create an account"}
      </button>
    </p>
  </div>
</main>
