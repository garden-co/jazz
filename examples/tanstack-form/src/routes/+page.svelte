<script lang="ts">
  import { page } from '$app/stores';
  import CreateOrder from '$lib/components/CreateOrder.svelte';
  import EditOrder from '$lib/components/EditOrder.svelte';
  import Orders from '$lib/components/Orders.svelte';

  // Get current route from URL hash or default to '/'
  const currentRoute = $derived($page.url.hash || '#/');
  const route = $derived(currentRoute.replace('#', '') || '/');
</script>

<main class="max-w-xl mx-auto px-3 py-8 space-y-8">
  {#if route === '/'}
    <Orders />
  {:else if route === '/order'}
    <CreateOrder />
  {:else if route.startsWith('/order/')}
    {#if route.split('/').length === 3}
      <EditOrder id={route.split('/')[2]} />
    {/if}
  {/if}
</main> 
