<script lang="ts">
  import Form from "$lib/components/Form.svelte";
  import { getUserAge, JazzAccount } from "$lib/schema";
  import { AccountCoState } from "jazz-tools/svelte";

  const account = new AccountCoState(JazzAccount, {
    resolve: {
      profile: true,
      root: true,
    },
  });
  const me = $derived(account.current);
</script>

<div class="text-center">
  <h1>
    Welcome{#if me?.profile?.firstName}, {me?.profile.firstName}{/if}!
  </h1>
  {#if me?.root}
    <p>As of today, you are {getUserAge(me?.root)} years old.</p>
  {/if}
</div>

<Form />

<p class="text-center">
  Edit the form above,
  <button type="button" onclick={() => window.location.reload()} class="font-semibold underline">
    refresh
  </button>
  this page, and see your changes persist.
</p>

<p class="text-center">
  Edit <code class="font-semibold">schema.ts</code> to add more fields.
</p>

<p class="text-center my-16">
  Go to
  <a class="font-semibold underline" href="https://jazz.tools/docs"> jazz.tools/docs </a>
  for our docs.
</p>
