<script lang="ts">
  import { path, navigate } from '@/lib/router';
  import CreateChatRedirect from '@/components/CreateChatRedirect.svelte';
  import ChatView from '@/components/chat-view/ChatView.svelte';
  import ChatList from '@/components/chat-list/ChatList.svelte';

  let currentPath = $state(window.location.hash.replace(/^#/, '') || window.location.pathname);

  $effect(() => {
    const unsub = path.subscribe((p) => {
      currentPath = p;
    });
    return () => unsub();
  });

  interface RouteMatch {
    id: string | null;
    params: Record<string, string>;
  }

  const routeMatch = $derived.by((): RouteMatch => {
    const p = currentPath;
    const chatMatch = p.match(/^\/chat\/([^/]+)$/);
    if (chatMatch?.[1]) {
      return { id: 'chat/:id', params: { id: decodeURIComponent(chatMatch[1]) } };
    }
    if (p === '/chats') return { id: 'chats', params: {} };
    return { id: '/', params: {} };
  });

  function handleClick(event: MouseEvent) {
    const link = (event.target as HTMLElement).closest('a');
    if (
      !link ||
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey
    )
      return;
    const href = link.getAttribute('href');
    if (!href || href.startsWith('http') || link.target === '_blank') return;
    event.preventDefault();
    if (href.startsWith('#')) {
      window.location.hash = href;
    } else {
      navigate(href);
    }
  }

  $effect(() => {
    window.addEventListener('click', handleClick);
    return () => window.removeEventListener('click', handleClick);
  });
</script>

{#if routeMatch.id === '/'}
  <CreateChatRedirect />
{:else if routeMatch.id === 'chat/:id'}
  <ChatView chatId={routeMatch.params.id ?? ''} />
{:else if routeMatch.id === 'chats'}
  <ChatList />
{:else}
  <div class="p-8">Not found</div>
{/if}
