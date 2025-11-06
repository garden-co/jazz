<script lang="ts">
  import BubbleBody from '$lib/components/BubbleBody.svelte';
  import BubbleContainer from '$lib/components/BubbleContainer.svelte';
  import BubbleImage from '$lib/components/BubbleImage.svelte';
  import BubbleInfo from '$lib/components/BubbleInfo.svelte';
  import BubbleText from '$lib/components/BubbleText.svelte';
  import type { Message } from '$lib/schema';
  import type { Account, Loaded, MaybeLoaded } from 'jazz-tools';
  let {
    me,
    msg
  }: {
    me: MaybeLoaded<Loaded<typeof Account>>;
    msg: Loaded<typeof Message, { text: true }>;
  } = $props();
  const lastEdit = $derived(msg.$jazz.getEdits().text);
  const fromMe = $derived(lastEdit?.by?.isMe);
  const { text, image } = $derived(msg);
</script>

{#if me.$isLoaded && (!me.canRead(msg) || !msg.text?.toString())}
  <BubbleContainer fromMe={false}>
    <BubbleBody fromMe={false}>
      <BubbleText text="Message not readable" className="italic text-gray-500"></BubbleText>
    </BubbleBody>
  </BubbleContainer>
{:else}
  <BubbleContainer {fromMe}>
    <BubbleBody {fromMe}>
      {#if image?.$isLoaded}
        <BubbleImage {image} />{/if}
      <BubbleText {text} />
    </BubbleBody>
    {#if lastEdit?.by?.profile?.$isLoaded}
      <BubbleInfo by={lastEdit.by.profile.name} madeAt={lastEdit.madeAt} />
    {/if}
  </BubbleContainer>
{/if}
