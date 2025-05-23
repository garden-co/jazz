<script lang="ts">
  let { children } = $props();
</script>

<div class="flex flex-col-reverse flex-1 overflow-y-auto" role="application">
  {@render children()}
</div>
