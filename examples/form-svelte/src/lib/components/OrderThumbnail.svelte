<script lang="ts">
  import { type Loaded } from 'jazz-tools';
  import { type BubbleTeaOrder } from '$lib/schema';

  type Props = {
    order: Loaded<typeof BubbleTeaOrder> | {
      baseTea: string;
      addOns: string[];
      deliveryDate: Date;
      withMilk: boolean;
      instructions?: string;
    };
  };

  let { order }: Props = $props();
</script>

<div class="border p-3 bg-gray-50 dark:bg-gray-800 rounded">
  <strong>
    {order.baseTea || "(No tea selected)"}
    {order.withMilk ? " milk " : " "}
    tea
  </strong>
  {#if order.addOns && order.addOns.length > 0}
    <p class="text-sm text-stone-600">
      with {order.addOns.join(", ").toLowerCase()}
    </p>
  {/if}
  {#if order.instructions}
    <p class="text-sm text-stone-600 italic">
      {order.instructions}
    </p>
  {/if}
  {#if order.deliveryDate}
    <p class="text-sm text-stone-600">
      Delivery: {order.deliveryDate.toLocaleDateString()}
    </p>
  {/if}
</div> 
