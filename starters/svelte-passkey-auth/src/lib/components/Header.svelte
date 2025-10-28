<script lang="ts">
  import { JazzAccount } from "$lib/schema";
  import { usePasskeyAuth } from "jazz-tools/svelte";
  import { AccountCoState } from "jazz-tools/svelte";

  let { appName } = $props();

  const { logOut } = new AccountCoState(JazzAccount);

  const { current, state } = $derived(
    usePasskeyAuth({
      appName,
    }),
  );

  const isAuthenticated = $derived(state === "signedIn");
</script>

<header>
  <nav class="flex justify-between items-center">
    {#if isAuthenticated}
      <span>You're logged in.</span>
    {:else}
      <span>Authenticate to share the data with another device.</span>
    {/if}

    {#if isAuthenticated}
      <button type="button" onclick={logOut} class="bg-stone-100 py-1.5 px-3 text-sm rounded-md">
        Log out
      </button>
    {:else}
      <div class="flex gap-2">
        <button
          type="button"
          class="bg-stone-100 py-1.5 px-3 text-sm rounded-md"
          onclick={() => current.signUp("")}
        >
          Sign up
        </button>
        <button
          type="button"
          class="bg-stone-100 py-1.5 px-3 text-sm rounded-md"
          onclick={() => current.logIn()}
        >
          Log in
        </button>
      </div>
    {/if}
  </nav>
</header>
