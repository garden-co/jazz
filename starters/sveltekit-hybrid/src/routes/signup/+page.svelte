<script lang="ts">
  import { goto } from "$app/navigation";
  import { getDb } from "jazz-tools/svelte";
  import { authClient } from "$lib/auth-client";

  const db = getDb();

  let error = $state<string | null>(null);

  async function handleSubmit(e: SubmitEvent) {
    e.preventDefault();
    error = null;
    const formData = new FormData(e.currentTarget as HTMLFormElement);
    const name = formData.get("name") as string;
    const email = formData.get("email") as string;
    const password = formData.get("password") as string;

    // Sign a short-lived token bound to the browser's current anonymous
    // Jazz identity. The server's BetterAuth hook verifies it and reuses
    // the proved user id when creating the BetterAuth user, so todos
    // created anonymously carry over to the new account.
    const proofToken = await db.getLocalFirstIdentityProof({
      ttlSeconds: 60,
      audience: "sveltekit-localfirst-signup",
    });
    if (!proofToken) {
      error = "Sign up requires an active Jazz session";
      return;
    }

    const res = await authClient.signUp.email({
      email,
      name,
      password,
      proofToken,
    } as Parameters<typeof authClient.signUp.email>[0]);

    if (res.error) {
      error = res.error.message ?? "Sign-up failed";
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
