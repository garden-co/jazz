<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";
  import { BrowserAuthSecretStore } from "jazz-tools/svelte";
  import TodoWidget from "$lib/TodoWidget.svelte";
  import AuthBackup from "$lib/AuthBackup.svelte";

  const session = authClient.useSession();

  async function handleSignOut() {
    // Clear the anonymous secret before signOut so that the reactive $effect
    // in the layout gets a fresh anonymous identity when it rebuilds the client.
    await BrowserAuthSecretStore.clearSecret();
    await authClient.signOut();
    await goto("/");
  }
</script>

<main class="dashboard">
  <header>
    <img src="/jazz.svg" alt="Jazz" class="wordmark" />
    <div class="auth-nav">
      {#if $session.data?.session}
        <p>Hello, {$session.data.user.name}</p>
        <button type="button" class="btn-secondary" onclick={handleSignOut}>Sign out</button>
      {:else}
        <p><a href="/signup" class="link">Sign up</a> or <a href="/signin" class="link">Sign in</a></p>
      {/if}
    </div>
  </header>
  <TodoWidget />
  {#if !$session.data?.session}
    <AuthBackup />
  {/if}
</main>
