<script lang="ts">
  import { AccountCoState } from "jazz-tools/svelte";
  import { JazzFestAccount } from "$lib/schema";
  const me = new AccountCoState(JazzFestAccount, {
    resolve: { root: { myFestival: {
      $each: true
    } } }
  });
</script>

<ul>
  {#each me.current.$isLoaded ? me.current.root.myFestival : [] as band}
    {#if band.$isLoaded}
      <li>{band.name}</li>
    {/if}
  {/each}
</ul>

{#if me.current.$isLoaded}
  <a href={`/festival/${me.current.root.myFestival.$jazz.id}`}>
    Go to my Server-Rendered Festival Page!
  </a>
{/if}
