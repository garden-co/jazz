<script lang="ts">
  import { AccountCoState } from "jazz-tools/svelte";
  import { MyAppAccount } from "./schema";
  import ChatPreview from "./ChatPreview.svelte";
  const me = new AccountCoState(MyAppAccount, {
    resolve: {
      profile: true,
      root: {
        myChats: { $each: true },
      },
    },
  });
</script>

    <div>
      <h1>Dashboard</h1>
      {#if me.current?.$isLoaded}
        <div>
          <p>Logged in as {me.current.profile.name}</p>
          <h2>My chats</h2>
          {#each me.current.root.myChats as chat (chat.$jazz.id)}
            <ChatPreview id={chat.$jazz.id} />
          {/each}
        </div>
        {:else}
        <div>Loading...</div>
        {/if}
    </div>
  );
}
