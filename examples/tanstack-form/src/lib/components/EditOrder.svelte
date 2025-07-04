<script lang="ts">
  import { CoState } from 'jazz-tools/svelte';
  import LinkToHome from './LinkToHome.svelte';
  import OrderFormWithSaveButton from './OrderFormWithSaveButton.svelte';
  import OrderThumbnail from './OrderThumbnail.svelte';
  import { BubbleTeaOrder } from '$lib/schema';

  type Props = {
    id: string;
  };

  let { id }: Props = $props();

  const order = new CoState(BubbleTeaOrder, id, {
    resolve: { addOns: true, instructions: true },
  });
</script>

{#if order.current}
  <div>
    <LinkToHome />

    <div class="mb-6">
      <p>Saved order:</p>
      <OrderThumbnail order={order.current} />
    </div>

    <h1 class="text-lg mb-4">
      <strong>Edit your bubble tea order 🧋</strong>
    </h1>

    <OrderFormWithSaveButton order={order.current} />
  </div>
{:else}
  <div>
    <LinkToHome />
    <p>Loading order...</p>
  </div>
{/if} 
