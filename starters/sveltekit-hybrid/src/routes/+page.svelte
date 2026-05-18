<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";
  import { LocalFirstAuth } from "jazz-tools/svelte";
  import TodoWidget from "$lib/TodoWidget.svelte";
  import AuthBackup from "$lib/AuthBackup.svelte";

  const session = authClient.useSession();
  // Auto-syncs with the layout's LocalFirstAuth instance via the shared
  // per-store notifier, so signOut here clears the secret everywhere.
  const auth = new LocalFirstAuth();

  async function handleSignOut() {
    await auth.signOut();
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
