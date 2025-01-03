<script lang="ts" module>
  export type Props = {
    depth?: DepthsIn<RegisteredAccount>;
    setResult: (result: ReturnType<typeof useAccount> | undefined) => void;
  };
</script>

<script lang="ts">
  import { useAccount, type RegisteredAccount } from '$lib/jazz.svelte.js';
  import type { DepthsIn } from 'jazz-tools';

  let { depth, setResult }: Props = $props();

  if (depth) {
    const result = $derived(useAccount(depth));

    $effect(() => {
      setResult(result);
    });
  } else {
    const result = $derived(useAccount());
  
    $effect(() => {
      setResult(result);
    });
  }
</script>
