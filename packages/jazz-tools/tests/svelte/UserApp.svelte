<script lang="ts">
  import type { Readable } from "svelte/store";
  import JazzProvider from "../../src/svelte/JazzProvider.svelte";
  let { user }: { user: Readable<string | null | undefined> } = $props();
</script>
<JazzProvider user={$user} autoAttachDevTools={false}>
  {#snippet signedOut()}<p>SIGN IN</p>{/snippet}
  {#snippet loading()}<p>WAIT</p>{/snippet}
  {#snippet children()}<p>PRIVATE</p>{/snippet}
</JazzProvider>
