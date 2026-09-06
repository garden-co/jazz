<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";
  import TodoWidget from "$lib/TodoWidget.svelte";
  import { getJazzLifecycle } from "$lib/jazz-lifecycle";

  const session = authClient.useSession();
  const lifecycle = getJazzLifecycle();

  async function handleSignOut() {
    try {
      await lifecycle.transition(async (accounts) => { await authClient.signOut(); accounts.logout(); });
      await goto("/");
    } catch (cause) {
      lifecycle.reportFailure(cause);
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
