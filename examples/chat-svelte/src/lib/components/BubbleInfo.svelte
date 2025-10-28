<script lang="ts">
  let { by, madeAt }: { by: string | undefined; madeAt: Date } = $props();
</script>

<div class="text-xs text-neutral-500 mt-1.5">
  {by} · {madeAt.toLocaleTimeString()}
</div>
