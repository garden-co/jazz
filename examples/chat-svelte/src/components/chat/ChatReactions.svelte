<script lang="ts">
  import type { CoFeed, CoFeedEntry, ID } from 'jazz-tools';
  import { cn } from '@/lib/utils';
  import type { ChatAccount } from '@/lib/schema';

  interface Props {
    currentUserId: ID<typeof ChatAccount> | undefined;
    isMe: boolean;
    reactions: CoFeed<string>;
    onToggle: (emoji: string) => void;
  }

  let { currentUserId, isMe, reactions, onToggle }: Props = $props();

  const reactionEntries = $derived.by(() => {
    const perAccount = reactions.perAccount ?? {};
    const acc: Record<string, CoFeedEntry<string>[]> = {};
    for (const item of Object.values(perAccount)) {
      const key = item.value;
      if (!acc[key]) acc[key] = [];
      acc[key].push(item);
    }
    return Object.entries(acc)
      .filter(([, list]) => list && list.length > 0)
      .sort(([a], [b]) => a.localeCompare(b));
  });
</script>

{#if reactionEntries.length > 0}
  <div
    class={cn(
      'absolute bottom-0 z-10 flex w-fit translate-y-[90%] gap-1',
      isMe ? 'right-0' : 'left-0'
    )}
  >
    {#each reactionEntries as [emoji, instances]}
      {@const count = instances.length ?? 0}
      {@const iReacted = instances.some((r) => r.by?.$jazz.id === currentUserId) ?? false}
      <button
        type="button"
        class={cn(
          'text-nowrap rounded-full border px-1.5 py-0.5 text-xs shadow-sm transition-colors',
          iReacted
            ? 'border-primary-foreground bg-primary text-primary-foreground'
            : 'bg-background hover:bg-muted'
        )}
        onclick={(e) => {
          e.stopPropagation();
          onToggle(emoji);
        }}
        onpointerdown={(e) => e.stopPropagation()}
      >
        {emoji}
        {#if count > 1}
          <span class="ml-1 opacity-75">{count}</span>
        {/if}
      </button>
    {/each}
  </div>
{/if}
