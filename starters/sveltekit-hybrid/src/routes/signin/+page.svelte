<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";

  let error = $state<string | null>(null);

  async function handleSubmit(e: SubmitEvent) {
    e.preventDefault();
    error = null;
    const data = new FormData(e.currentTarget as HTMLFormElement);
    const email = data.get("email") as string;
    const password = data.get("password") as string;

    const res = await authClient.signIn.email({ email, password });
    if (res.error) {
      error = res.error.message ?? "Sign-in failed";
      return;
    }
    // Refresh the reactive session store so the layout observes
    // authenticated=true before we navigate — otherwise the home page
    // briefly mounts against the old anonymous Jazz client.
    await authClient.getSession();
    await goto("/");
  }
</script>

<main class="page-center">
  <img src="/jazz.svg" alt="Jazz" class="wordmark" />
  <div class="card">
    <h1>Sign in</h1>
    <form onsubmit={handleSubmit}>
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
      <button type="submit" class="btn-primary"> Sign in </button>
    </form>
  </div>
</main>
