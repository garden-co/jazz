<!-- #region auth-external-svelte -->
<script lang="ts">
  import type { Snippet } from "svelte";
  import { JazzProvider, sessionState, type JazzClient, type JazzSession } from "jazz-tools/svelte";
  // A session you opened with await createJazzClient({ serverUrl: "/jazz" }).
  let { session, children }: { session: JazzSession<JazzClient>; children: Snippet } = $props();
  const state = $derived(sessionState(session));
  // After your own sign-in has set its session cookie, ask your backend who is signed in.
  function signIn() { void state.refresh().catch(() => {}); }
</script>
<JazzProvider {session}>
  {@render children()}
  {#snippet signedOut()}
    <button disabled={!!$state.pending} onclick={signIn}>Sign in</button>
    {#if $state.error}<p role="alert">{$state.error.message}</p>{/if}
  {/snippet}
</JazzProvider>
<!-- #endregion auth-external-svelte -->
