<script lang="ts" module>
  export type Props = {
    depth?: DepthsIn<RegisteredAccount>;
    setResult: (result: ReturnType<typeof useAccountOrGuest> | undefined) => void;
  };
</script>

<script lang="ts">
  import { useAccountOrGuest, type RegisteredAccount } from '$lib/jazz.svelte.js';
  import type { DepthsIn } from 'jazz-tools';

  let { depth, setResult }: Props = $props();

  if (depth) {
    const result = $derived(useAccountOrGuest(depth));

    $effect(() => {
      setResult(result);
    });
  } else {
    const result = $derived(useAccountOrGuest());
  
    $effect(() => {
      setResult(result);
    });
  }
</script>
