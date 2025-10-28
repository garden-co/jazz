<script lang="ts">
  import { goto } from '$app/navigation';
  import { Chat } from '$lib/schema';
  import { AccountCoState } from 'jazz-tools/svelte';
  import { Account, Group } from 'jazz-tools';

  const account = new AccountCoState(Account);
  const me = $derived(account.current);
  $effect(() => {
    if (!me) return;
    const group = Group.create();
    group.addMember('everyone', 'writer');
    const chat = Chat.create([], group);
    goto(`/chat/${chat.$jazz.id}`);
  });
</script>
