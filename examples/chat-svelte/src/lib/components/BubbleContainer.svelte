<script lang="ts">
  import type { Snippet } from 'svelte';
  let { children, fromMe }: { children: Snippet; fromMe: boolean | undefined } = $props();
  const align = fromMe ? 'items-end' : 'items-start';
</script>

<div class="{align} flex flex-col m-3" role="row">
  {@render children()}
</div>
