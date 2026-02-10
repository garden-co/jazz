<script lang="ts">
  import type { ID } from 'jazz-tools';
  import { CoState } from 'jazz-tools/svelte';
  import { Lock, MessageSquare, Trash2 } from 'lucide-svelte';
  import * as AlertDialog from '@/components/ui/alert-dialog';
  import { Button } from '@/components/ui/button';
  import * as Item from '@/components/ui/item';
  import { navigate } from '@/lib/router';
  import { Chat } from '@/lib/schema';

  interface Props {
    chatId: ID<typeof Chat>;
    onDelete: () => void;
  }

  let { chatId, onDelete }: Props = $props();

  const chatState = $derived(new CoState(Chat, chatId));
  const chat = $derived(chatState.current);

  const dateStr = $derived.by(() => {
    if (!chat.$isLoaded) return '';
    return new Date(chat.$jazz.createdAt).toLocaleString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: 'numeric',
      timeZoneName: 'short',
      hour12: true,
      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone
    });
  });
  const isPublicComputed = $derived.by(() => {
    if (!chat.$isLoaded) return false;
    return !!chat.$jazz.owner.getRoleOf('everyone');
  });

  function goToChat(e: MouseEvent) {
    e.stopPropagation();
    if (chat.$isLoaded) navigate(`#/chat/${chat.$jazz.id}`);
  }

  function stopPropagation(e: MouseEvent) {
    e.stopPropagation();
  }
</script>

{#if chat.$isLoaded}
  <Item.Root class="bg-background cursor-pointer" variant="outline" size="sm" onclick={goToChat}>
    <Item.ItemMedia>
      {#if isPublicComputed}
        <MessageSquare class="size-4" />
      {:else}
        <Lock class="size-4" />
      {/if}
    </Item.ItemMedia>
    <Item.ItemContent>
      <Item.ItemTitle>{dateStr}</Item.ItemTitle>
      <Item.ItemDescription>
        {isPublicComputed ? 'Public ' : 'Private '}chat
      </Item.ItemDescription>
    </Item.ItemContent>
    <Item.ItemActions>
      <AlertDialog.AlertDialog>
        <AlertDialog.AlertDialogTrigger onclick={stopPropagation}>
          {#snippet child({ props })}
            <Button variant="destructive" {...props}>
              <Trash2 class="size-4" />
            </Button>
          {/snippet}
        </AlertDialog.AlertDialogTrigger>
        <AlertDialog.AlertDialogContent>
          <AlertDialog.AlertDialogHeader>
            <AlertDialog.AlertDialogTitle>Are you absolutely sure?</AlertDialog.AlertDialogTitle>
            <AlertDialog.AlertDialogDescription>
              You will no longer see this chat in your list, but others will still be able to access
              it.
            </AlertDialog.AlertDialogDescription>
          </AlertDialog.AlertDialogHeader>
          <AlertDialog.AlertDialogFooter>
            <AlertDialog.AlertDialogCancel onclick={stopPropagation}>
              Cancel
            </AlertDialog.AlertDialogCancel>
            <AlertDialog.AlertDialogAction
              class="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onclick={(e) => {
                e.stopPropagation();
                e.preventDefault();
                onDelete();
              }}
            >
              Continue
            </AlertDialog.AlertDialogAction>
          </AlertDialog.AlertDialogFooter>
        </AlertDialog.AlertDialogContent>
      </AlertDialog.AlertDialog>
    </Item.ItemActions>
  </Item.Root>
{:else}
  <div class="p-3 border rounded-md animate-pulse bg-muted/30">
    <div class="h-4 w-32 bg-muted-foreground/20 rounded"></div>
  </div>
{/if}
