<script lang="ts">
  import { AccountCoState } from 'jazz-tools/svelte';
  import { JazzAccount, BubbleTeaOrder, BubbleTeaBaseTeaTypes, ListOfBubbleTeaAddOns } from '$lib/schema';
  import OrderFormWithSaveButton from './OrderFormWithSaveButton.svelte';
  import LinkToHome from './LinkToHome.svelte';
  import { CoPlainText } from 'jazz-tools';

  const me = new AccountCoState(JazzAccount, {
    resolve: {
      profile: true,
      root: {
        orders: true
      }
    }
  });

  const orders = $derived(me.current?.root.orders);

  // Create a new order when component loads
  let newOrder = $state<any>(null);

  $effect(() => {
    if (orders && !newOrder) {
      const order = BubbleTeaOrder.create({
        baseTea: BubbleTeaBaseTeaTypes[0],
        addOns: ListOfBubbleTeaAddOns.create([], orders._owner),
        deliveryDate: new Date(),
        withMilk: false,
        instructions: CoPlainText.create(''),
      }, orders._owner);

      orders.push(order);
      newOrder = order;
    }
  });
</script>

<div>
  <LinkToHome />

  <h1 class="text-lg mb-4">
    <strong>Create your bubble tea order 🧋</strong>
  </h1>

  {#if newOrder}
    <OrderFormWithSaveButton order={newOrder} />
  {:else}
    <p>Creating order...</p>
  {/if}
</div> 
