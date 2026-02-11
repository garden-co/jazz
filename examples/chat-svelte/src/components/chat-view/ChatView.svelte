<script lang="ts">
  import { deleteCoValues } from 'jazz-tools';
  import type { ID } from 'jazz-tools';
  import { AccountCoState, CoState } from 'jazz-tools/svelte';
  import { Loader2 } from 'lucide-svelte';
  import MessageComposer from '@/components/composer/MessageComposer.svelte';
  import { Button } from '@/components/ui/button';
  import { Chat, ChatAccount, Message } from '@/lib/schema';
  import ChatMessage from '@/components/chat/ChatMessage.svelte';
  import { toast } from 'svelte-sonner';
  import { navigate } from '@/lib/router';

  const INITIAL_MESSAGES_TO_SHOW = 20;
  const LOAD_MORE_STEP = 20;

  interface Props {
    chatId: string;
  }

  let { chatId }: Props = $props();

  let showNLastMessages = $state(INITIAL_MESSAGES_TO_SHOW);

  const account = new AccountCoState(ChatAccount, {
    resolve: { root: { chats: true }, profile: true }
  });

  const me = $derived(account.current);

  const chatState = $derived(new CoState(Chat, chatId));
  const chat = $derived(chatState.current);

  $effect(() => {
    if (!me.$isLoaded || !chat.$isLoaded) return;
    if (!me.root.chats.$jazz.has(chatId)) {
      me.root.chats.$jazz.set(chatId, chat);
    }
  });

  const messageIds = $derived(
    chat.$isLoaded
      ? [...chat.$jazz.refs]
          .slice(-showNLastMessages)
          .reverse()
          .map((r) => r.id)
      : []
  );

  const chatLength = $derived(chat.$isLoaded ? chat.length : 0);
  const hasMore = $derived(chatLength > showNLastMessages);

  async function onDelete(id: ID<typeof Message>) {
    if (!chat.$isLoaded) return;
    chat.$jazz.remove((msg) => msg.$jazz.id === id);
    await deleteCoValues(Message, id);
    toast.success('Message deleted');
  }

  let loadMoreButton = $state<HTMLElement | null>(null);

  $effect(() => {
    if (!loadMoreButton || !hasMore) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) {
          showNLastMessages += LOAD_MORE_STEP;
        }
      },
      {
        rootMargin: '200px'
      }
    );

    observer.observe(loadMoreButton);

    return () => observer.disconnect();
  });
</script>

{#if chat.$isLoaded}
  <div class="flex h-full flex-1 flex-col">
    <div class="flex flex-1 flex-col-reverse gap-8 overflow-y-auto p-2 pb-6">
      {#if messageIds.length > 0}
        {#each messageIds as messageId (messageId)}
          <ChatMessage {messageId} {onDelete} />
        {/each}
      {:else}
        <div class="flex flex-col items-center justify-center py-10">
          <p class="text-muted-foreground text-sm">No messages yet</p>
        </div>
      {/if}

      {#if hasMore}
        <div bind:this={loadMoreButton}>
          <Button variant="ghost" onclick={() => (showNLastMessages += LOAD_MORE_STEP)}>
            Load older messages
          </Button>
        </div>
      {/if}
    </div>

    <MessageComposer {chat} />
  </div>

  <!-- Error states below -->
{:else if me.$jazz.loadingState === 'loading'}
  <div class="flex flex-col items-center justify-center py-10">
    <Loader2 class="size-6 animate-spin text-muted-foreground" />
  </div>

  <!-- Error states below -->
{:else if me.$jazz.loadingState === 'deleted'}
  {@render error({
    title: 'Your account data has been deleted',
    body: 'The account data associated with your session no longer exists. Please log out and sign in again to continue.',
    actions: [{ label: 'Log out', onClick: account.logOut }]
  })}
{:else if me.$jazz.loadingState === 'unauthorized'}
  {@render error({
    title: `You don't have access to this chat`,
    body: '',
    actions: [{ label: 'Go to home page', onClick: () => navigate('/') }]
  })}
{:else if chat.$jazz.loadingState === 'deleted'}
  {@render error({
    title: 'The chat you are trying to access has been permanently deleted',
    body: '',
    actions: [{ label: 'Go to home page', onClick: () => navigate('/') }]
  })}
{:else if me.$jazz.loadingState === 'unavailable'}
  {@render error({
    title: 'The chat you are trying to access is unavailable',
    body: 'This means either the chat does not exist or it is not available to you at the moment. You may need to connect to the internet or try again later.',
    actions: [
      { label: 'Retry', onClick: () => window.location.reload() },
      { label: 'Go to home page', onClick: () => navigate('/') }
    ]
  })}
{:else}
  {@render error({
    title: 'Something went wrong',
    body: 'The chat could not be loaded.',
    actions: [
      { label: 'Go to home page', onClick: () => navigate('/') },
      { label: 'Log out', onClick: () => account.logOut(), variant: 'destructive' }
    ]
  })}
{/if}

{#snippet error({
  title,
  body,
  actions
}: {
  title: string;
  body: string;
  actions: {
    label: string;
    onClick: () => void;
    variant?: 'link' | 'default' | 'destructive' | 'outline' | 'secondary' | 'ghost' | undefined;
  }[];
})}
  <div class="flex min-h-screen items-center justify-center p-8">
    <div class="max-w-2xl space-y-4">
      <h1 class="text-2xl font-semibold text-red-600">{title}</h1>
      <p class="text-muted-foreground">
        {body}
      </p>
      {#each actions as action}
        <Button variant={action.variant} onclick={action.onClick}>{action.label}</Button>
      {/each}
    </div>
  </div>
{/snippet}
