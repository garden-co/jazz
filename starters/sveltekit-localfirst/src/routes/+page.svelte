<script lang="ts">
  import TodoWidget from "$lib/TodoWidget.svelte";
  import AuthBackup from "$lib/AuthBackup.svelte";
</script>

<main class="dashboard">
  <header>
    <img src="/jazz.svg" alt="Jazz" class="wordmark" />
  </header>
  <TodoWidget />
  <AuthBackup />
</main>
