<script lang="ts">
  import { goto } from '$app/navigation';
  import { Chat } from '$lib/schema';
  import { AccountCoState } from 'jazz-tools/svelte';
  import { co } from 'jazz-tools';

  const account = new AccountCoState(co.account());
  const me = $derived(account.current);
  $effect(() => {
    if (!me.$isLoaded) return;
    const group = co.group().create();
    group.addMember('everyone', 'writer');
    const chat = Chat.create([], group);
    goto(`/chat/${chat.$jazz.id}`);
  });
</script>
