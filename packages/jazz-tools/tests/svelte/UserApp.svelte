<script lang="ts">
  import type { Snippet } from "svelte";
  import JazzProvider from "../../src/svelte/JazzProvider.svelte";
  import AuthStatus from "./AuthStatus.svelte";
  import type { JazzSessionState } from "../../src/svelte/session-state.js";
  let { onAuth, children: privateChildren }: { onAuth: (session: JazzSessionState) => void; children: Snippet } = $props();
</script>
<JazzProvider config={{ scope: "test", initial: "signed-out" }}>
  {#snippet signedOut()}<p>SIGN IN</p><div hidden><AuthStatus {onAuth} /></div>{/snippet}
  {#snippet loading()}<p>WAIT</p>{/snippet}
  {#snippet children()}{@render privateChildren()}{/snippet}
</JazzProvider>
