<script lang="ts">
  import { co } from 'jazz-tools';
  import { AccountCoState } from 'jazz-tools/svelte';
  import { Lock, MessageSquarePlus } from 'lucide-svelte';
  import ChatListItem from '@/components/chat-list/ChatListItem.svelte';
  import { Button } from '@/components/ui/button';
  import { navigate } from '@/lib/router';
  import { Chat, ChatAccount } from '@/lib/schema';

  const account = new AccountCoState(ChatAccount, {
    resolve: {
      root: {
        chats: { $each: true }
      }
    }
  });
  const me = $derived(account.current);

  const chats = $derived(
    me.$isLoaded
      ? Object.values(me.root.chats).sort(
          (a, b) =>
            new Date(b.$jazz.createdAt || 0).getTime() - new Date(a.$jazz.createdAt || 0).getTime()
        )
      : []
  );

  async function createPrivateChat() {
    if (!me.$isLoaded) return;
    const privateGroup = co.group().create();
    const chat = Chat.create(
      [
        {
          text: 'This is a private chat.',
          reactions: []
        }
      ],
      { owner: privateGroup }
    );
    me.root.chats.$jazz.set(chat.$jazz.id, chat);
    navigate(`#/chat/${chat.$jazz.id}`);
  }
</script>

<div class="p-2 flex flex-col gap-2">
  <div class="grid grid-cols-2 gap-2">
    <Button onclick={() => navigate('#/')}>
      <MessageSquarePlus class="size-4" /> New Chat
    </Button>
    <Button variant="outline" onclick={createPrivateChat}>
      <Lock class="size-4" /> New Private Chat
    </Button>
  </div>

  {#if me.$isLoaded}
    {#each chats as chat}
      <ChatListItem
        chatId={chat.$jazz.id}
        onDelete={() => me.root.chats.$jazz.delete(chat.$jazz.id)}
      />
    {/each}
  {:else}
    <p class="text-muted-foreground text-sm p-4">Loading...</p>
  {/if}
</div>
