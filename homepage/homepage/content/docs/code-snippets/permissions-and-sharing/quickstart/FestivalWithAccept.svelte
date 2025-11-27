<script lang="ts">
  // [!code ++:3]
  import { AccountCoState, createInviteLink, InviteListener } from "jazz-tools/svelte";
  // @ts-expect-error Not available outside SK app
  import { goto } from "$app/navigation";
  import { Festival, JazzFestAccount } from "$lib/schema";

  const me = new AccountCoState(JazzFestAccount, {
    resolve: { root: { myFestival: { $each: true } } }
  });
  let inviteLink = $state<string | null>(null);

  // [!code ++:6]
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
  {#if me.current.$isLoaded}
    {#each me.current.root.myFestival as band}
      <li>{band.name}</li>
    {/each}
  {/if}
</ul>

<input type="text" bind:value={inviteLink} readonly />
<button onclick={inviteLinkClickHandler}>
  Create Invite Link
</button>
