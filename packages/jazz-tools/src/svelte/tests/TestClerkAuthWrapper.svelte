<script lang="ts">
  import type { MinimalClerkClient } from "jazz-tools";
  import { useClerkAuth } from "../auth/ClerkAuth.svelte.js";

  type Props = {
    clerk: MinimalClerkClient;
  };

  let { clerk }: Props = $props();

  const auth = useClerkAuth(clerk);
</script>

<div data-testid="clerk-auth-wrapper">
  <div data-testid="auth-state">{auth.state}</div>
</div>
