<script lang="ts">
  import type { Snippet } from "svelte";
  import { JazzProvider, betterAuth } from "jazz-tools/svelte";
  import { env } from "$env/dynamic/public";
  import { authClient } from "$lib/auth-client";
  import SignIn from "$lib/SignIn.svelte";

  let { children: pageChildren }: { children?: Snippet } = $props();
</script>

<JazzProvider appId={env.PUBLIC_JAZZ_APP_ID!} serverUrl={env.PUBLIC_JAZZ_SERVER_URL!} auth={betterAuth(authClient)}>
  {#snippet signedOut()}<SignIn />{/snippet}
  {#snippet children()}{@render pageChildren?.()}{/snippet}
</JazzProvider>
