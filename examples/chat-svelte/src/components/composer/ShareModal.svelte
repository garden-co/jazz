<script lang="ts">
  import type { ID } from 'jazz-tools';
  import type { Chat } from '@/lib/schema';
  import QR from '@svelte-put/qr/svg/QR.svelte';
  import { Check, Copy, Share2 } from 'lucide-svelte';
  import { Button } from '@/components/ui/button';
  import * as Dialog from '@/components/ui/dialog';
  import { Input } from '@/components/ui/input';
  import { toast } from 'svelte-sonner';

  interface Props {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    chatId: ID<typeof Chat>;
    inviteLink: string;
  }

  let { open, onOpenChange, inviteLink }: Props = $props();

  let copied = $state(false);

  async function handleCopy() {
    await navigator.clipboard.writeText(inviteLink);
    copied = true;
    setTimeout(() => (copied = false), 2000);
  }

  async function handleShare() {
    if (navigator.share) {
      try {
        await navigator.share({
          title: 'Join my chat',
          text: "I'm inviting you to a secure chat on Jazz.",
          url: inviteLink
        });
      } catch (err) {
        toast.error('Failed to share');
        console.error('Error sharing:', err);
      }
    }
  }
</script>

<Dialog.Root {open} {onOpenChange}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>Invite to chat</Dialog.Title>
    </Dialog.Header>

    <Dialog.Description>
      Share the link below with your friends to invite them to join the chat, or have them scan the
      QR code to join automatically.
    </Dialog.Description>

    {#if inviteLink}
      <div class="flex flex-col items-center gap-4 py-4">
        <div class="flex justify-center">
          <QR data={inviteLink} width={256} height={256} logo="/jazz-logo.svg" correction="Q" />
        </div>
        <div class="flex w-full flex-col gap-2">
          <div class="flex w-full items-center gap-2">
            <Input id="link" value={inviteLink} readonly />
            <Button size="icon" variant="outline" onclick={handleCopy}>
              <span class="sr-only">Copy</span>
              {#if copied}
                <Check class="size-4" />
              {:else}
                <Copy class="size-4" />
              {/if}
            </Button>
          </div>

          {#if typeof navigator !== 'undefined' && 'share' in navigator}
            <Button variant="outline" class="w-full gap-2" onclick={handleShare}>
              <Share2 class="size-4" />
              Share Link
            </Button>
          {/if}
        </div>
      </div>
    {:else}
      <div class="py-4 text-center">
        <p>Preparing invite...</p>
      </div>
    {/if}

    <Dialog.Footer>
      <Button onclick={() => onOpenChange(false)}>Done</Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
