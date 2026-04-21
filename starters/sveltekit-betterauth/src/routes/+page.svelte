<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";

  let isSignUp = $state(false);
  let error = $state<string | null>(null);

  async function handleSubmit(e: SubmitEvent) {
    e.preventDefault();
    error = null;
    const data = new FormData(e.currentTarget as HTMLFormElement);
    const email = data.get("email") as string;
    const password = data.get("password") as string;
    const name = data.get("name") as string | null;

    const res = name
      ? await authClient.signUp.email({ name, email, password })
      : await authClient.signIn.email({ email, password });

    if (res.error) {
      error = res.error.message ?? (name ? "Sign-up failed" : "Sign-in failed");
      return;
    }
    await goto("/dashboard");
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
          <input id="name" name="name" type="text" required />
        </div>
      {/if}
      <div class="field">
        <label for="email">Email</label>
        <input id="email" name="email" type="email" required />
      </div>
      <div class="field">
        <label for="password">Password</label>
        <input id="password" name="password" type="password" required />
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
      <button type="button" class="link" onclick={() => { isSignUp = !isSignUp; error = null; }}>
        {isSignUp ? "Sign in" : "Create an account"}
      </button>
    </p>
  </div>
</main>
