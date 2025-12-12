<script lang="ts">
  import { co, ID } from "jazz-tools";
  import OrderForm from "./OrderForm.svelte";
  import { BubbleTeaOrder, PartialBubbleTeaOrder } from "./schema";
  import { CoState } from "jazz-tools/svelte";
  const { orderId } : { orderId : ID<typeof BubbleTeaOrder> } = $props();
  const order = new CoState(BubbleTeaOrder, orderId);
</script>

{#if order.current.$isLoaded}
  <OrderForm order={order.current} />
{/if}
