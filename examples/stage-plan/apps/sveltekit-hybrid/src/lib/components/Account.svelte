<script lang="ts">
  import { env } from "$env/dynamic/public";
  import { useJazzAuth } from "jazz-tools/svelte";
  import { authClient } from "$lib/auth-client";
  import { backendIssuer, LOCAL_APP_ID } from "$lib/config";

  const jazzAuth = useJazzAuth();
  const betterAuthSession = authClient.useSession();
  const issuer = backendIssuer(env.PUBLIC_JAZZ_APP_ID || LOCAL_APP_ID);
  const signedIn = $derived($jazzAuth.account?.identity.issuer === issuer);

  let mode = $state<"closed" | "sign-up" | "sign-in">("closed");
  let name = $state("");
  let email = $state("");
  let password = $state("");
  let error = $state<string>();
  // Set while this component changes accounts, so the restore below never
  // races a sign-up (a racing login would give the new identity a fresh
  // account before sign-up links it to the guest's).
  let changing = $state(false);

  async function change(run: () => Promise<void>) {
    changing = true;
    error = undefined;
    try {
      await run();
      mode = "closed";
      password = "";
    } catch (cause) {
      error = cause instanceof Error ? cause.message : String(cause);
    } finally {
      changing = false;
    }
  }

  // Creating an account links it to the guest's local-first account, so every
  // show and task made so far simply stays.
  const signUp = () =>
    change(async () => {
      const result = await authClient.signUp.email({ name: name.trim() || email, email, password });
      if (result.error) throw new Error(result.error.message ?? "Could not create the account.");
      await jazzAuth.sessionActions.linkWithBackend();
    });

  // Signing in opens the existing account. What this browser made as a
  // guest stays with the guest account.
  const signIn = () =>
    change(async () => {
      const result = await authClient.signIn.email({ email, password });
      if (result.error) throw new Error(result.error.message ?? "Could not sign in.");
      await jazzAuth.sessionActions.loginWithBackend();
    });

  const signOut = () =>
    change(async () => {
      await jazzAuth.logout();
      await authClient.signOut();
      // A fresh guest: the next person on this device starts empty.
      await jazzAuth.sessionActions.createLocalFirst();
    });

  // A Jazz session is not kept across reloads for a signed-in account: when
  // Better Auth still has a session, sign back in through the backend.
  $effect(() => {
    if ($betterAuthSession.isPending || changing || signedIn) return;
    if ($betterAuthSession.data?.user) void change(() => jazzAuth.sessionActions.loginWithBackend());
  });
</script>

<section class="account" aria-label="Account">
  {#if signedIn}
    <span class="secondary">Signed in as {$betterAuthSession.data?.user.email}</span>
    <button class="ghost" disabled={changing} onclick={signOut}>Sign out</button>
  {:else if mode === "closed"}
    <span class="secondary">Guest: your board is saved on this device.</span>
    <button class="primary" onclick={() => (mode = "sign-up")}>Create an account</button>
    <button class="ghost" onclick={() => (mode = "sign-in")}>Sign in</button>
  {:else}
    <form
      class="card"
      onsubmit={(event) => {
        event.preventDefault();
        void (mode === "sign-up" ? signUp() : signIn());
      }}
    >
      <h2>{mode === "sign-up" ? "Create an account" : "Sign in"}</h2>
      {#if mode === "sign-up"}
        <label>Name <input bind:value={name} autocomplete="name" /></label>
      {/if}
      <label>Email <input type="email" bind:value={email} required autocomplete="email" /></label>
      <label>
        Password
        <input
          type="password"
          bind:value={password}
          required
          minlength="8"
          autocomplete={mode === "sign-up" ? "new-password" : "current-password"}
        />
      </label>
      <button class="primary" type="submit" disabled={changing}>
        {mode === "sign-up" ? "Create account" : "Sign in"}
      </button>
      <button class="ghost" type="button" onclick={() => (mode = "closed")}>Cancel</button>
      {#if error}<p class="error" role="alert">{error}</p>{/if}
    </form>
  {/if}
</section>
