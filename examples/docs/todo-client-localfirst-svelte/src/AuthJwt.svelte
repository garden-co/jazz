<!-- #region auth-jwt-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { JazzProvider } from "jazz-tools/svelte";
  // Your backend names the user (its authenticate reads your session cookie).
  // In `signedOut`, call getJazzSession().refresh() after your own sign-in.
  let { children, signedOut }: { children: Snippet; signedOut: Snippet } = $props();
</script>
<JazzProvider config={{ serverUrl: "/jazz" }} {signedOut}>{@render children()}</JazzProvider>
<!-- #endregion auth-jwt-svelte -->
