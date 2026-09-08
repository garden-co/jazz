<script lang="ts">
  import type { Readable } from "svelte/store";
  import type { Snippet } from "svelte";
  import type { JazzAuth } from "../../src/session/app.js";
  import JazzProvider from "../../src/svelte/JazzProvider.svelte";
  let { auth, children: privateChildren }: { auth: Readable<JazzAuth>; children: Snippet } = $props();
</script>
<JazzProvider appId="test" auth={$auth} autoAttachDevTools={false}>
  {#snippet signedOut()}<p>SIGN IN</p>{/snippet}
  {#snippet loading()}<p>WAIT</p>{/snippet}
  {#snippet children()}{@render privateChildren()}{/snippet}
</JazzProvider>
