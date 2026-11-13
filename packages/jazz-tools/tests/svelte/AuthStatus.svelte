<script lang="ts">
  import { untrack } from "svelte";
  import { getJazzSession, type JazzSessionState } from '../../src/svelte/session-state.js';
  let { onAuth }: { onAuth?: (session: JazzSessionState) => void } = $props();
  const session = getJazzSession();
  untrack(() => onAuth?.(session));
</script>
<p>{$session.status}</p>
<button onclick={() => void session.signOut().catch(() => {})}>Sign out</button>
