<script lang="ts">
  import { goto } from "$app/navigation";
  import { authClient } from "$lib/auth-client";
  import TodoWidget from "$lib/TodoWidget.svelte";

  const session = authClient.useSession();

  async function handleSignOut() {
    await authClient.signOut();
    await goto("/");
  }
</script>

{#if $session.data}
  <main class="dashboard">
    <header>
      <img src="/jazz.svg" alt="Jazz" class="wordmark" />
      <p>Hello, {$session.data.user.name}</p>
      <button type="button" onclick={handleSignOut}>Sign out</button>
    </header>
    <TodoWidget />
  </main>
{/if}
