<script lang="ts">
  // [!code ++:2]
  import { AccountCoState, CoState } from "jazz-tools/svelte";
  import { JazzFestAccount, Festival } from "$lib/schema";

	const { id }: { id?: string } = $props();
  const me = new AccountCoState(JazzFestAccount, {
    resolve: { root: { myFestival: true } }
  });
  let name = $state("");

    // [!code ++:2]
  const festivalId = $derived(id ?? (me.current.$isLoaded && me.current.root.$isLoaded ? me.current.root.myFestival.$jazz.id : undefined));
	const festival = $derived(new CoState(Festival, festivalId));

  function handleSave() {
    // [!code --:2]
    if (!me.current.$isLoaded) return;
    me.current.root.myFestival.$jazz.push({ name });
    // [!code ++:2]
    if (!festival.current.$isLoaded) return;
    festival.current.$jazz.push({ name });
    name = "";
  }
</script>

<div>
  <input type="text" bind:value={name} placeholder="Band name" />
  <button type="button" onclick={handleSave}>Add</button>
</div>
