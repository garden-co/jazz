<script lang="ts">
import type { Snippet } from "svelte";
import { usePasskeyAuth } from "./PasskeyAuth.svelte.js";

let { appName, children }: { appName: string; children?: Snippet } = $props();

const auth = usePasskeyAuth({ appName });

let error = $state<string | undefined>(undefined);

function signUp(e: Event) {
  const formData = new FormData(e.currentTarget as HTMLFormElement);
  const name = formData.get("name") as string;

  if (!name) {
    error = "Name is required";
    return;
  }
  e.preventDefault();
  error = undefined;
  auth.current.signUp(name).catch((e) => {
    error = e.message;
  });
}

function logIn(e: Event) {
  error = undefined;
  e.preventDefault();
  e.stopPropagation();
  auth.current.logIn().catch((e) => {
    error = e.message;
  });
}
</script>

{#if auth.state === 'anonymous'}
  <div
    style="width: 100vw; height: 100vh; display: flex; align-items: center; justify-content: center;"
  >
    <div style="max-width: 18rem; display: flex; flex-direction: column; gap: 2rem;">
      {#if error}
        <div style="color: red;">
          {error}
        </div>
      {/if}
      <form onsubmit={signUp}>
        <input type="text" name="name" placeholder="Display name" autocomplete="name" />
        <input type="submit" value="Sign up" />
      </form>
      <button onclick={logIn}> Log in with existing account </button>
    </div>
  </div>
{:else}
  {@render children?.()}
{/if}

<style>
  form {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  button,
  input[type='submit'] {
    background: #000;
    color: #fff;
    padding: 6px 12px;
    border: none;
    border-radius: 6px;
    min-height: 38px;
    cursor: pointer;
  }

  input[type='text'] {
    border: 2px solid #000;
    padding: 6px 12px;
    border-radius: 6px;
    min-height: 24px;
  }
</style>
