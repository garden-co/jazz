<script lang="ts">
  let { children } = $props();
</script>

<div
  class="flex justify-between w-full gap-2 p-3 bg-white border-b border-stone-200 dark:bg-transparent dark:border-stone-900"
>
  {@render children()}
</div>
