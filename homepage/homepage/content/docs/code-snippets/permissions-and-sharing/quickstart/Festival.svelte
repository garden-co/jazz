<script lang="ts">
  // [!code ++:1]
  import { AccountCoState, CoState, createInviteLink, InviteListener } from "jazz-tools/svelte";
  // @ts-expect-error Only in a real SK app
  import { goto } from "$app/navigation";
  import { Festival, JazzFestAccount } from "$lib/schema";

  // [!code ++:1]
	const { id }: { id?: string } = $props();
  const me = new AccountCoState(JazzFestAccount, {
    resolve: { root: { myFestival: true } }
  });
  // [!code ++:2]
  const festivalId = $derived(id ?? (me.current.$isLoaded ? me.current.root.myFestival.$jazz.id : undefined));
	const festival = $derived(new CoState(Festival, festivalId));
  let inviteLink = $state<string | null>(null);

  new InviteListener({
		invitedObjectSchema: Festival,
		onAccept: (festivalID) => {
			goto(`/festival/${festivalID}`);
		}
	});

  function inviteLinkClickHandler() {
    if (!me.current.$isLoaded) return;
    const link = createInviteLink(me.current?.root.myFestival, "writer")
    inviteLink = link;
  }
</script>

<ul>
  {#if me.current.$isLoaded && festival.current.$isLoaded}
    <!-- [!code ++:1] -->
    {#each festival.current as band}
      <li>{band.$isLoaded ? band.name : null}</li>
    {/each}
  {/if}
</ul>

{#if me.current.$isLoaded && festival.current.$isLoaded}
  {#if me.current.canAdmin(festival.current)}
    <input type="text" bind:value={inviteLink} readonly />
    <button onclick={inviteLinkClickHandler}>
      Create Invite Link
    </button>
  {/if}
{/if}
