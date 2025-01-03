<script lang="ts" module>
  export type Props<R extends CoValue> = {
    Schema: CoValueClass<R>;
    id: ID<R>;
    depth: DepthsIn<R>;
    setResult: (result: R | undefined) => void;
  };
</script>

<script lang="ts" generics="R extends CoValue">
  import { useCoState } from '$lib/jazz.svelte.js';
  import type { CoValue, CoValueClass, DepthsIn, ID } from 'jazz-tools';

  let { Schema, id, depth, setResult }: Props<R> = $props();

  const result = $derived(useCoState(Schema, id, depth));

  $effect(() => {
    setResult(result.current);
  });
</script>
