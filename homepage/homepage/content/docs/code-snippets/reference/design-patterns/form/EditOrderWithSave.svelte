<script lang="ts">
  import { co, ID } from "jazz-tools";
  import { CoState } from "jazz-tools/svelte";
  import { BubbleTeaOrder } from "./schema";
  import OrderForm from "./OrderForm.svelte";

  const owner = co.group().create();
  const { id }:  { id: ID<typeof BubbleTeaOrder>} = $props();

  const order = new CoState(BubbleTeaOrder, id, {
    resolve: {
      addOns: { $each: true, $onError: "catch" },
      instructions: true,
    },
    unstable_branch: {
      name: "edit-order",
      owner,
    },
  });

  function handleSave(e: Event) {
    e.preventDefault();
    if (!order.current.$isLoaded) return;

    // Merge the branch back to the original
    order.current.$jazz.unstable_merge();
    // Navigate away or show success message
  }

  function handleCancel() {
    // Navigate away without saving - the branch will be discarded
  }
</script>

{#if order.current.$isLoaded}
  <OrderForm order={order.current} onSave={handleSave} />
{/if}
