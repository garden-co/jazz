<script lang="ts">
  import { createInviteLink } from 'jazz-tools/svelte';
  import { AccountCoState, CoState } from 'jazz-tools/svelte';
  import { Brush, CloudUpload, Image, Plus, Share2 } from 'lucide-svelte';
  import ShareModal from '@/components/composer/ShareModal.svelte';
  import UploadModal from '@/components/composer/UploadModal.svelte';
  import { Button } from '@/components/ui/button';
  import * as DropdownMenu from '@/components/ui/dropdown-menu';
  import { Canvas, CanvasAttachment, Chat, ChatAccount, type Attachment } from '@/lib/schema';
  import type { co } from 'jazz-tools';

  interface Props {
    onAddAttachment: (attachment: co.loaded<typeof Attachment>) => void;
    chatId: string;
  }

  let { onAddAttachment, chatId }: Props = $props();

  const chatState = $derived(new CoState(Chat, chatId));
  const chat = $derived(chatState.current);
  const account = new AccountCoState(ChatAccount);
  const me = $derived(account.current);

  let intent = $state<'image' | 'file' | null>(null);
  let willShare = $state(false);
  let menuOpen = $state(false);
  let inviteLink = $state('');

  const canShare = $derived(
    me.$isLoaded && chat.$isLoaded && (me.canAdmin(chat) || chat.$jazz.owner.getRoleOf('everyone'))
  );

  const chatOwner = $derived(chat.$isLoaded ? chat.$jazz.owner : undefined);
</script>

{#if chat.$isLoaded}
  <DropdownMenu.Root open={menuOpen} onOpenChange={(v) => (menuOpen = v ?? false)}>
    <DropdownMenu.Trigger>
      {#snippet child({ props })}
        <Button variant="outline" size="icon-lg" class="rounded-full" {...props}>
          <Plus />
        </Button>
      {/snippet}
    </DropdownMenu.Trigger>

    <DropdownMenu.Content>
      <DropdownMenu.Item onSelect={() => (intent = 'image')}>
        <Image />
        Image
      </DropdownMenu.Item>

      <DropdownMenu.Item onSelect={() => (intent = 'file')}>
        <CloudUpload />
        File
      </DropdownMenu.Item>

      <DropdownMenu.Item
        onSelect={() => {
          if (!chatOwner) return;
          const canvas = Canvas.create({}, { owner: chatOwner });
          const attachment = CanvasAttachment.create(
            { type: 'canvas', name: 'New Canvas', canvas },
            { owner: chatOwner }
          );
          onAddAttachment(attachment);
        }}
      >
        <Brush />
        Canvas
      </DropdownMenu.Item>

      {#if canShare}
        <DropdownMenu.Item
          onSelect={() => {
            if (!inviteLink) {
              if (me.$isLoaded && me.canAdmin(chat)) {
                inviteLink = createInviteLink(chat, 'writer', {
                  baseURL: `${window.location.origin}/`
                });
              } else {
                inviteLink = window.location.href;
              }
            }
            willShare = true;
          }}
        >
          <Share2 />
          Invite to chat
        </DropdownMenu.Item>
      {/if}
    </DropdownMenu.Content>
  </DropdownMenu.Root>

  <UploadModal
    owner={chatOwner}
    open={!!intent}
    onOpenChange={(isOpen) => !isOpen && (intent = null)}
    title={intent === 'image' ? 'Upload image' : 'Upload file'}
    accept={intent === 'image' ? 'image/*' : undefined}
    onUpload={(file) => {
      onAddAttachment(file);
      intent = null;
    }}
  />

  {#if canShare}
    <ShareModal
      chatId={chat.$jazz.id}
      open={willShare}
      {inviteLink}
      onOpenChange={(v) => (willShare = v)}
    />
  {/if}
{/if}
