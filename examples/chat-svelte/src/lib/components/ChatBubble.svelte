<script lang="ts">
  import BubbleBody from '$lib/components/BubbleBody.svelte';
  import BubbleContainer from '$lib/components/BubbleContainer.svelte';
  import BubbleImage from '$lib/components/BubbleImage.svelte';
  import BubbleInfo from '$lib/components/BubbleInfo.svelte';
  import BubbleText from '$lib/components/BubbleText.svelte';
  import type { Message } from '$lib/schema';
  import type { Account, Loaded } from 'jazz-tools';
  let {
    me,
    msg
  }: {
    me: Loaded<typeof Account> | null | undefined;
    msg: Loaded<typeof Message>;
  } = $props();
  const lastEdit = $derived(msg._edits.text);
  const fromMe = $derived(lastEdit?.by?.isMe);
  const { text, image } = $derived(msg);
</script>

{#if me && (!me.canRead(msg) || !msg.text?.toString())}
  <BubbleContainer fromMe={false}>
    <BubbleBody fromMe={false}>
      <BubbleText text="Message not readable" className="italic text-gray-500"></BubbleText>
    </BubbleBody>
  </BubbleContainer>
{:else}
  <BubbleContainer {fromMe}>
    <BubbleBody {fromMe}>
      {#if image}
        <BubbleImage {image} />{/if}
      <BubbleText {text} />
    </BubbleBody>
    <BubbleInfo by={lastEdit?.by?.profile?.name} madeAt={lastEdit?.madeAt || new Date()} />
  </BubbleContainer>
{/if}
