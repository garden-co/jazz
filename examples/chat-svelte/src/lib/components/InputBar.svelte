<script lang="ts">
  let { children } = $props();
</script>

<div
  class="flex gap-1 p-3 mt-auto bg-white border-t shadow-2xl border-stone-200 dark:bg-transparent dark:border-stone-900"
>
  {@render children()}
</div>
