<script lang="ts">
  import "../app.css";
  import { JazzSessionProvider } from "jazz-tools/svelte";
  import { env } from "$env/dynamic/public";
  import SessionStatus from "$lib/SessionStatus.svelte";
  import AuthBackup from "$lib/AuthBackup.svelte";
  let { children: pageChildren } = $props();
  const appId = env.PUBLIC_JAZZ_APP_ID;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
</script>

{#if appId && serverUrl}
<JazzSessionProvider config={{ appId, serverUrl, initial: "local-first" }}>
  {#snippet children()}
    <main class="dashboard">
      <header><img src="/jazz.svg" alt="Jazz" class="wordmark" /></header>
      {@render pageChildren?.()}
      <AuthBackup />
    </main>
  {/snippet}
  {#snippet fallback()}<SessionStatus />{/snippet}
</JazzSessionProvider>
{:else}<p role="alert">PUBLIC_JAZZ_APP_ID and PUBLIC_JAZZ_SERVER_URL must be set</p>{/if}
