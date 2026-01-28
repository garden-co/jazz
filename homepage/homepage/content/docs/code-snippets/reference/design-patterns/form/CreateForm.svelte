<script lang="ts">
  import { co, ID } from "jazz-tools";
  import OrderForm from "./OrderForm.svelte";
  import { BubbleTeaOrder, JazzAccount, PartialBubbleTeaOrder } from "./schema";
  import { CoState, AccountCoState } from "jazz-tools/svelte";
  const { id }: { id: ID<typeof BubbleTeaOrder> } = $props();

  const me = new AccountCoState(JazzAccount, {
    resolve: {
      root: {
        orders: true
      }
    }
  });

  const newOrder = new CoState(PartialBubbleTeaOrder, id);

  const handleSave = async () => {
    if (me.current.$isLoaded && newOrder.current.$isLoaded) me.current.root.orders.$jazz.push(newOrder.current as co.loaded<typeof BubbleTeaOrder>);
  };

</script>

{#if newOrder.current.$isLoaded}
  <OrderForm order={newOrder.current} onSave={handleSave} />
{/if}
