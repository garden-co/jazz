<script lang="ts">
import { usePasskeyAuth } from "jazz-svelte";
import { AccountCoState } from "jazz-svelte";

let { appName } = $props();

const me = new AccountCoState();

const { current, state } = usePasskeyAuth({
  appName,
});

const isAuthenticated = $derived(state === "signedIn");

$inspect(me);
</script>

<header>
  <nav class="flex justify-between items-center">
    {#if isAuthenticated}
      <span>You're logged in.</span>
    {:else}
      <span>Authenticate to share the data with another device.</span>
    {/if}

    {#if isAuthenticated}
      <button type="button" onclick={me.logOut} class="bg-stone-100 py-1.5 px-3 text-sm rounded-md">
        Log out
      </button>
    {:else}
      <div class="flex gap-2">
        <button
          type="button"
          class="bg-stone-100 py-1.5 px-3 text-sm rounded-md"
          onclick={() => current.signUp('')}
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
