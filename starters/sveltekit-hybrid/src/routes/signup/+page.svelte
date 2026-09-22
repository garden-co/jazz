<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";
  import { credential } from "$lib/accounts";
  import { getJazzSession } from "jazz-tools/svelte";

  const jazz = getJazzSession();


  let error = $state<string | null>(null);

  async function handleSubmit(e: SubmitEvent) {
    e.preventDefault();
    error = null;
    const formData = new FormData(e.currentTarget as HTMLFormElement);
    const name = formData.get("name") as string;
    const email = formData.get("email") as string;
    const password = formData.get("password") as string;

    const res = await authClient.signUp.email({
      email,
      name,
      password,
    } as Parameters<typeof authClient.signUp.email>[0]);

    if (res.error) {
      error = res.error.message ?? "Sign-up failed";
      return;
    }

    try {
      await jazz.linkJWT({ getToken: credential });
    } catch (cause) {
      error = cause instanceof Error ? cause.message : "Sign-up failed";
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
    <h1>Create account</h1>
    <form onsubmit={handleSubmit}>
      <div class="field">
        <label for="name">Name</label>
        <input id="name" name="name" type="text" required />
      </div>
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
        Create account
      </button>
    </form>
  </div>
</main>
