<script lang="ts">
  import type { Snippet } from "svelte";
  import JazzProvider from "../../src/svelte/JazzProvider.svelte";
  import AuthStatus from "./AuthStatus.svelte";
  import type { JazzAuthState } from "../../src/svelte/auth-state.js";
  let { onAuth, children: privateChildren }: { onAuth: (auth: JazzAuthState) => void; children: Snippet } = $props();
</script>
<JazzProvider appId="test" initial="signed-out">
  {#snippet signedOut()}<p>SIGN IN</p><div hidden><AuthStatus {onAuth} /></div>{/snippet}
  {#snippet loading()}<p>WAIT</p>{/snippet}
  {#snippet children()}{@render privateChildren()}{/snippet}
</JazzProvider>
