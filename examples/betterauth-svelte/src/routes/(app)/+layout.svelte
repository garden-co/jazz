<script>
  import Navbar from "$lib/components/Navbar.svelte";
  const { children } = $props();
</script>

<Navbar />
<div
  class="container mx-auto pt-16 min-h-screen flex flex-col items-center justify-center"
>
  {@render children?.()}
</div>
