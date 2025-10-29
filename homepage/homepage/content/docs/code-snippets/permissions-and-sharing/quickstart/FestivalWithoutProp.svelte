<script lang="ts">
  // [!code ++:1]
  import { AccountCoState, createInviteLink } from "jazz-tools/svelte";
  import { JazzFestAccount } from "$lib/schema";

  const me = new AccountCoState(JazzFestAccount, {
    resolve: { root: { myFestival: { $each: true } } }
  });
  // [!code ++:7]
  let inviteLink = $state<string | null>(null);

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
