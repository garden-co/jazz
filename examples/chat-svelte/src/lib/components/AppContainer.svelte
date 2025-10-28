<script lang="ts">
  let { children } = $props();
</script>

<div
  class="flex flex-col justify-between w-screen h-screen bg-stone-50 dark:bg-stone-925 dark:text-white"
>
  {@render children()}
</div>
