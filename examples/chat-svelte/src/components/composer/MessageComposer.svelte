<script lang="ts">
  import type { co } from 'jazz-tools';
  import { toast } from 'svelte-sonner';
  import { Send } from 'lucide-svelte';
  import ActionMenu from '@/components/composer/ActionMenu.svelte';
  import Editor from '@/components/editor/Editor.svelte';
  import { Button } from '@/components/ui/button';
  import { Message, type Attachment, type Chat } from '@/lib/schema';

  interface Props {
    chat: co.loaded<typeof Chat>;
  }

  let { chat }: Props = $props();

  let currentMessage = $state<co.loaded<typeof Message>>();
  let lastChatId = $state('');

  $effect(() => {
    if (!chat.$isLoaded) return;
    const chatId = chat.$jazz.id;
    if (lastChatId !== chatId) {
      lastChatId = chatId;
      currentMessage = Message.create({ text: '', reactions: [] }, { owner: chat.$jazz.owner });
    }
  });

  function handleSend() {
    if (!currentMessage || !chat.$isLoaded) return;
    const text = currentMessage.text.trim();
    const hasAttachment = currentMessage.attachment != null;
    if (!text && !hasAttachment) return;
    chat.$jazz.push(currentMessage);
    currentMessage = Message.create({ text: '', reactions: [] }, { owner: chat.$jazz.owner });
  }

  async function handleAttachmentUpload(attachment: co.loaded<typeof Attachment>) {
    if (!chat.$isLoaded) return;
    try {
      const newMessage = Message.create(
        {
          text: '',
          reactions: [],
          attachment
        },
        { owner: chat.$jazz.owner }
      );
      chat.$jazz.push(newMessage);
    } catch (err) {
      console.error(err);
      toast.error("Couldn't upload the file");
    }
  }
</script>

<div class="m-2 flex items-end gap-2">
  <ActionMenu onAddAttachment={handleAttachmentUpload} chatId={chat.$jazz.id} />

  {#if currentMessage}
    <Editor message={currentMessage} onEnter={handleSend} />
  {/if}

  <Button variant="outline" size="icon-lg" onclick={handleSend}>
    <Send data-testid="send-message" />
  </Button>
</div>
