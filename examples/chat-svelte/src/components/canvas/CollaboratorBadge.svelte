<script lang="ts">
  import { CoState } from 'jazz-tools/svelte';
  import { ChatAccount } from '@/lib/schema';

  interface Props {
    accountId: string;
    color: string;
  }

  let { accountId, color }: Props = $props();

  const accountState = new CoState(ChatAccount, () => accountId, {
    resolve: { profile: true }
  });
  const user = $derived(accountState.current);
</script>

{#if user.$isLoaded}
  <span class="flex items-center gap-2">
    <span
      class="inline-block size-3 rounded-full border border-stone-200"
      style="background-color: {color}"
    ></span>
    <span>{user.profile.name}</span>
  </span>
{/if}
