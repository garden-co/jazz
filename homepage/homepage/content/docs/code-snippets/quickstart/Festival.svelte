<script lang="ts">
  import { AccountCoState } from "jazz-tools/svelte";
  import { JazzFestAccount } from "$lib/schema";
  const me = new AccountCoState(JazzFestAccount, {
    resolve: { root: { myFestival: true } }
  });
</script>

<ul>
  {#each me.current?.root.myFestival || [] as band}
    <li>{band?.name}</li>
  {/each}
</ul>
