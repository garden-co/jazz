<script lang="ts">
  import "../app.css";
  import { authClient } from "$lib/auth-client";
  import JazzClientProvider from "$lib/JazzClientProvider.svelte";

  let { children: pageChildren } = $props();

  const session = authClient.useSession();
  const authenticated = $derived(
    $session.isPending ? null : Boolean($session.data?.session),
  );
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
