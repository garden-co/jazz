<script lang="ts" module>
  export type Props<R extends CoValue> = {
    invitedObjectSchema: CoValueClass<R>;
    onAccept: (id: ID<R>) => void;
  };
</script>

<script lang="ts" generics="R extends CoValue">
  import { useAcceptInvite } from '$lib/jazz.svelte.js';
  import type { CoValue, CoValueClass, ID } from 'jazz-tools';

  let { invitedObjectSchema, onAccept }: Props<R> = $props();

  useAcceptInvite({
    invitedObjectSchema, onAccept
  });
</script>
