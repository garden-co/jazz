<script lang="ts">
	import { AccountCoState } from "jazz-tools/svelte";
	import { JazzFestAccount } from "$lib/schema";

  const me = new AccountCoState(JazzFestAccount, {
    resolve: { root: { myFestival: true } }
  });
  let name = $state("");

  function handleSave() {
    if (!me.current.$isLoaded) return;
    me.current.root.myFestival.$jazz.push({ name });
    name = "";
  }
</script>

<div>
  <input type="text" bind:value={name} placeholder="Band name" />
  <button type="button" onclick={handleSave}>Add</button>
</div>
