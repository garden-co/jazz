<script lang="ts">
  import { AccountCoState, InviteListener } from 'jazz-tools/svelte';
  import { toast } from 'svelte-sonner';
  import { Chat, ChatAccount, Message } from '@/lib/schema';
  import { navigate } from '@/lib/router';

  const account = new AccountCoState(ChatAccount, {
    resolve: { root: { chats: true } }
  });
  const me = $derived(account.current);

  let initialized = $state(false);

  new InviteListener({
    invitedObjectSchema: Chat,
    onAccept: async (chatId) => {
      const chat = await Chat.load(chatId);
      if (!chat.$isLoaded) toast.error('Failed to load chat');
      else navigate(`/chat/${chatId}`);
    }
  });

  $effect(() => {
    if (initialized || !me.$isLoaded) return;
    initialized = true;
    const chat = Chat.create([]);
    const helloMessage = Message.create(
      { text: 'Hello world', reactions: [] },
      { owner: chat.$jazz.owner }
    );
    chat.$jazz.push(helloMessage);
    me.root.chats.$jazz.set(chat.$jazz.id, chat);
    navigate(`/#/chat/${chat.$jazz.id}`);
  });
</script>

<div class="flex-1 overflow-y-auto flex flex-col-reverse">
  <article>Creating chat...</article>
</div>
