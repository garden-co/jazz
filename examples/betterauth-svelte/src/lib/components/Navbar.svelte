<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { betterAuthClient } from "$lib/auth-client";
  import { useIsAuthenticated } from "jazz-tools/svelte";
  import jazzLogo from "$lib/assets/jazz-logo.svg";

  const isAuthenticated = useIsAuthenticated();

  const signOut = () => {
    betterAuthClient.signOut().catch(console.error);
  };
</script>

<header class="absolute p-4 top-0 left-0 w-full z-10 flex justify-between">
  <nav class="flex gap-4">
    <a href="/">
      <img src={jazzLogo} alt="Jazz logo" width={96} height={96} />
    </a>
  </nav>
  <nav class="flex gap-4">
    {#if isAuthenticated.current}
      <Button onclick={signOut}>Sign out</Button>
    {:else}
      <Button asChild variant="secondary">
        <a href="/auth/sign-in">Sign in</a>
      </Button>
      <Button asChild>
        <a href="/auth/sign-up">Sign up</a>
      </Button>
    {/if}
  </nav>
</header>
