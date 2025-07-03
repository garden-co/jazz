<script lang="ts">
  import { AccountCoState } from 'jazz-tools/svelte';
  import { JazzAccount, type BubbleTeaOrder } from '$lib/schema';
  import { type Loaded } from 'jazz-tools';
  import OrderThumbnail from './OrderThumbnail.svelte';
  import { goto } from '$app/navigation';

  const me = new AccountCoState(JazzAccount, {
    resolve: {
      profile: true,
      root: {
        orders: {
          $each: true
        }
      }
    }
  });

  const orders = $derived(me.current?.root.orders);

  function navigateToCreateOrder() {
    goto('#/order');
  }

  function navigateToEditOrder(id: string) {
    goto(`#/order/${id}`);
  }
</script>

<div>
  <h1 class="text-lg mb-4">
    <strong>Your bubble tea orders 🧋</strong>
  </h1>

  {#if orders && orders.length > 0}
    <div class="space-y-4">
      {#each orders as order}
        {#if order}
          <button 
            type="button"
            class="border rounded-lg p-4 cursor-pointer hover:bg-gray-50 text-left w-full" 
            onclick={() => navigateToEditOrder(order.id)}
          >
            <OrderThumbnail {order} />
          </button>
        {/if}
      {/each}
    </div>
  {:else}
    <p class="text-gray-500">No orders yet. Create your first order!</p>
  {/if}

  <button
    onclick={navigateToCreateOrder}
    class="mt-6 bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
  >
    Create New Order
  </button>
</div> 
