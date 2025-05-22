<script lang="ts" module>
  export type Props<R extends CoValueOrZodSchema> = {
    invitedObjectSchema: R;
    onAccept: (id: ID<R>) => void;
  };
</script>

<script lang="ts" generics="R extends CoValueOrZodSchema">
  import { useAcceptInvite } from '../../jazz.svelte.js';
  import type { CoValueOrZodSchema, ID } from 'jazz-tools';

  let { invitedObjectSchema, onAccept }: Props<R> = $props();

  useAcceptInvite({
    invitedObjectSchema, onAccept
  });
</script>
