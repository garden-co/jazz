<script lang="ts">
  import { CoState } from 'jazz-tools/svelte';
  import { ChatAccountWithProfile } from '@/lib/schema';

  interface Props {
    date: number;
    sender?: string;
  }

  let { date, sender }: Props = $props();

  const senderState = $derived(new CoState(ChatAccountWithProfile, sender));
  const senderAccount = $derived(senderState.current);

  const dateStr = $derived(new Date(date).toLocaleTimeString());
</script>

<div class="text-muted-foreground mb-1 flex gap-1 text-xs">
  {#if senderAccount.$isLoaded}
    <span>{senderAccount.profile.name}</span>
  {/if}
  <span>&bull;</span>
  <span>{dateStr}</span>
</div>
