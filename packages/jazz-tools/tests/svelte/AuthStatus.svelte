<script lang="ts">
  import { untrack } from "svelte";
  import { useJazzAuth, type JazzAuthState } from '../../src/svelte/auth-state.js';
  let { onAuth }: { onAuth?: (auth: JazzAuthState) => void } = $props();
  const auth = useJazzAuth();
  untrack(() => onAuth?.(auth));
</script>
<p>{$auth.status}</p>
<button onclick={() => void auth.logout()}>Log out</button>
