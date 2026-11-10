<script lang="ts">
  import "@garden-co/design/jazz/tokens.css";
  import "@garden-co/design/jazz/fonts.css";
  import "../app.css";
  import type { Snippet } from "svelte";
  import { JazzProvider } from "jazz-tools/svelte";

  let { children }: { children: Snippet } = $props();
</script>

<!--
  Everyone starts on a local-first account, so the board works before
  sign-up and offline. Creating an account links a Better Auth identity to
  that same account through this app's backend (see Account.svelte).
-->
<JazzProvider
  serverUrl="/jazz"
  initial="local-first"
  client={() => import("jazz-tools/local-first")}
>
  {@render children()}
  {#snippet loading()}<p class="status" role="status">Opening StagePlan…</p>{/snippet}
  {#snippet signedOut()}<p class="status" role="status">Opening StagePlan…</p>{/snippet}
  {#snippet error(cause, retry)}
    <div class="status" role="alert">
      <p>Could not open StagePlan: {cause.message}</p>
      <button onclick={() => void retry()}>Try again</button>
    </div>
  {/snippet}
</JazzProvider>
