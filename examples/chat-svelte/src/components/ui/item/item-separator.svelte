<script lang="ts">
  import { Separator } from '@/components/ui/separator';
  import { cn } from '@/lib/utils';
  import type { ComponentProps } from 'svelte';

  let {
    ref = $bindable(null),
    class: className,
    ...restProps
  }: ComponentProps<typeof Separator> = $props();
</script>

<Separator
  bind:ref
  data-slot="item-separator"
  orientation="horizontal"
  class={cn('my-0', className)}
  {...restProps}
/>
