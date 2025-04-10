<script lang="ts" module>
  export type Props = {
    inboxOwnerID: ID<RegisteredAccount> | undefined;
  };
</script>

<script lang="ts">
  import type { ID } from 'jazz-tools';
  import { useInboxSender, type RegisteredAccount } from '../../index.js';

  let { inboxOwnerID }: Props = $props();
  let messageText = $state('');
  let error = $state('');

  // Use the hook
  const sendMessage = useInboxSender<{ text: string }, any>(inboxOwnerID);

  async function handleSend() {
    try {
      error = '';
      if (!messageText) return;

      const message = JSON.parse(messageText);
      await sendMessage(message);
      messageText = '';
    } catch (e) {
      error = (e as Error).message;
      throw e;
    }
  }
</script>

<div>
  <input data-testid="message-input" bind:value={messageText} placeholder="Enter message JSON" />
  <button data-testid="send-button" on:click={handleSend}> Send </button>

  {#if error}
    <div data-testid="error">{error}</div>
  {/if}
</div>
