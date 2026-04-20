<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";
  import { BrowserAuthSecretStore } from "jazz-tools/svelte";
  import TodoWidget from "$lib/TodoWidget.svelte";

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
        <span class="user-email">{$session.data.user.email}</span>
        <button type="button" class="btn-secondary" onclick={handleSignOut}>Sign out</button>
      {:else}
        <p><a href="/signup" class="btn-secondary">Sign up</a> or <a href="/signin" class="link">Sign in</a></p>
      {/if}
    </div>
  </header>
  <TodoWidget />
</main>
