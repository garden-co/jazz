<script lang="ts">
  import type { ID } from 'jazz-tools';
  import DOMPurify from 'dompurify';
  import { AccountCoState, CoState } from 'jazz-tools/svelte';
  import { Trash2 } from 'lucide-svelte';
  import ChatFile from '@/components/chat/ChatFile.svelte';
  import ChatImage from '@/components/chat/ChatImage.svelte';
  import ChatMetadata from '@/components/chat/ChatMetadata.svelte';
  import ChatReactionPicker from '@/components/chat/ChatReactionPicker.svelte';
  import ChatReactions from '@/components/chat/ChatReactions.svelte';
  import CollaborativeCanvas from '@/components/canvas/CollaborativeCanvas.svelte';
  import * as AlertDialog from '@/components/ui/alert-dialog';
  import * as DropdownMenu from '@/components/ui/dropdown-menu';
  import { Item, ItemContent } from '@/components/ui/item';
  import { cn } from '@/lib/utils';
  import { ChatAccount, Message } from '@/lib/schema';

  interface Props {
    messageId: ID<typeof Message>;
    onDelete: (id: ID<typeof Message>) => Promise<void>;
  }

  let { messageId, onDelete }: Props = $props();

  const account = new AccountCoState(ChatAccount);
  const msg = $derived(new CoState(Message, messageId));
  const message = $derived(msg.current);
  const me = $derived(account.current);
  const currentUserId = $derived(me.$isLoaded ? me.$jazz.id : undefined);
  const isMe = $derived(
    !!currentUserId && message.$isLoaded && message.$jazz.createdBy === currentUserId
  );

  const sanitised = $derived(message.$isLoaded && DOMPurify.sanitize(message.text.toString()));

  let isMenuOpen = $state(false);
  let isDeleteDialogOpen = $state(false);

  function handleEmojiSelect(emoji: string) {
    if (!message.$isLoaded) return;
    const current = message.reactions.byMe?.value;
    message.reactions.$jazz.push(current === emoji ? '' : emoji);
    isMenuOpen = false;
  }

  async function handleDeleteConfirm() {
    await onDelete(message.$jazz.id);
    isDeleteDialogOpen = false;
  }
</script>

{#if message.$isLoaded}
  <article
    class={cn('flex max-w-[7/8] flex-col', isMe ? 'self-end items-end' : 'self-start items-start')}
  >
    <ChatMetadata date={message.$jazz.createdAt} sender={message.$jazz.createdBy} />

    <DropdownMenu.Root open={isMenuOpen} onOpenChange={(v) => (isMenuOpen = v ?? false)}>
      <DropdownMenu.Trigger>
        <Item
          variant="outline"
          class={cn(
            'max-w-full inline-flex cursor-pointer select-none px-2 py-1 shadow-xs',
            isMe ? 'border-0 bg-primary text-primary-foreground' : 'bg-background'
          )}
        >
          <ItemContent class="relative mt-0 text-base">
            {#if message.attachment}
              {#if message.attachment.type === 'image' && message.attachment.attachment.$jazz.id}
                <div class="mb-2">
                  <ChatImage imageId={message.attachment.attachment.$jazz.id} />
                </div>
              {/if}
              {#if message.attachment.type === 'file' && message.attachment.attachment.$jazz.id}
                <div class="mb-2">
                  <ChatFile fileId={message.attachment.attachment.$jazz.id} />
                </div>
              {/if}
              {#if message.attachment.type === 'canvas' && message.attachment.canvas.$jazz.id}
                <div class="mb-2">
                  <CollaborativeCanvas
                    canvasId={message.attachment.canvas.$jazz.id}
                    showControls={true}
                    class="w-full"
                  />
                </div>
              {/if}
            {/if}

            <div class="wrap-anywhere max-w-full whitespace-pre-line" aria-label="Message content">
              <!-- eslint-disable-next-line svelte/no-at-html-tags this string is sanitised -->
              {@html sanitised}
            </div>

            <ChatReactions
              {currentUserId}
              {isMe}
              reactions={message.reactions}
              onToggle={(emoji) => handleEmojiSelect(emoji)}
            />
          </ItemContent>
        </Item>
      </DropdownMenu.Trigger>
      <DropdownMenu.Content align={isMe ? 'end' : 'start'}>
        <ChatReactionPicker
          onPick={(emoji) => {
            handleEmojiSelect(emoji);
          }}
        />
        {#if isMe}
          <DropdownMenu.Item
            variant="destructive"
            onSelect={(e) => {
              e.preventDefault();
              isDeleteDialogOpen = true;
              isMenuOpen = false;
            }}
          >
            <Trash2 class="size-4" />
            Delete
          </DropdownMenu.Item>
        {/if}
      </DropdownMenu.Content>
    </DropdownMenu.Root>

    <AlertDialog.Root
      open={isDeleteDialogOpen}
      onOpenChange={(v) => (isDeleteDialogOpen = v ?? false)}
    >
      <AlertDialog.Content>
        <AlertDialog.Header>
          <AlertDialog.Title>Are you absolutely sure?</AlertDialog.Title>
          <AlertDialog.Description>
            This action cannot be undone. This will permanently delete your message from our
            servers.
          </AlertDialog.Description>
        </AlertDialog.Header>
        <AlertDialog.Footer>
          <AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
          <AlertDialog.Action
            class="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            onclick={handleDeleteConfirm}
          >
            Yes, delete it
          </AlertDialog.Action>
        </AlertDialog.Footer>
      </AlertDialog.Content>
    </AlertDialog.Root>
  </article>
{/if}
