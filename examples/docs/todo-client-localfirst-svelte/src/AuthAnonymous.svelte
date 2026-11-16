<!-- #region auth-anon-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { JazzProvider } from "jazz-tools/svelte";
  // Everyone starts as a guest: a device account, created on this device.
  // Your backend accepts guests with acceptDeviceAccounts.
  let { children }: { children: Snippet } = $props();
</script>
<JazzProvider config={{ serverUrl: "/jazz", initial: "device" }}>{@render children()}</JazzProvider>
<!-- #endregion auth-anon-svelte -->
