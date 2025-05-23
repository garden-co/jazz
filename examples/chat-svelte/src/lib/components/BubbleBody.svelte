<script lang="ts">
  import type { Snippet } from 'svelte';

  let { children, fromMe }: { children: Snippet; fromMe: boolean | undefined } = $props();
</script>

<div
  class="line-clamp-10 text-ellipsis rounded-2xl overflow-hidden max-w-[calc(100%-5rem)] shadow-sm p-1 {fromMe
    ? 'bg-white dark:bg-stone-900 dark:text-white'
    : 'bg-blue text-white'}"
>
  {@render children()}
</div>
