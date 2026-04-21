<script lang="ts">
  import "../app.css";
  import { authClient } from "$lib/auth-client";
  import JazzClientProvider from "$lib/JazzClientProvider.svelte";

  let { children: pageChildren } = $props();

  const session = authClient.useSession();

  // Never revert to null once resolved — prevents JazzClientProvider from
  // unmounting during BetterAuth background re-fetches (which set isPending:true
  // briefly), which would reset any in-progress UI state (e.g. open <details>).
  let authenticated = $state<boolean | null>(null);
  $effect(() => {
    if (!$session.isPending) {
      authenticated = Boolean($session.data?.session);
    }
  });
</script>

{#if authenticated !== null}
  {#key authenticated}
    <JazzClientProvider {authenticated}>
      {#snippet children()}
        {@render pageChildren?.()}
      {/snippet}
    </JazzClientProvider>
  {/key}
{/if}
