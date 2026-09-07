<script lang="ts">
  import { getJazzSession } from "jazz-tools/svelte";
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";
  import TodoWidget from "$lib/TodoWidget.svelte";
  import { getAuthActions } from "$lib/auth-actions";

  const jazz = getJazzSession();
  const session = authClient.useSession();
  const auth = getAuthActions();

  async function handleSignOut() {
    try {
      await jazz.logout();
      await authClient.signOut();
      await goto("/");
    } catch (cause) {
      auth.reportFailure(cause);
    }
  }
</script>

{#if $session.data}
  <main class="dashboard">
    <header>
      <img src="/jazz.svg" alt="Jazz" class="wordmark" />
      <div class="auth-nav">
        <p>Hello, {$session.data.user.name}</p>
        <button type="button" onclick={handleSignOut}>Sign out</button>
      </div>
    </header>
    <TodoWidget />
  </main>
{/if}
