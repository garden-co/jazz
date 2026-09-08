<script lang="ts">
  import { useJazzAuth } from '../../src/svelte/auth-state.js';
  const auth = useJazzAuth();
</script>
<p>{$auth.status}</p>
<button onclick={() => void auth.logout()}>Log out</button>
