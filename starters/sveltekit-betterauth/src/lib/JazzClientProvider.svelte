<script lang="ts">
  import { onMount, type Snippet } from "svelte";
  import { JazzSessionProvider, createJazzSession, connectBetterAuth, type JazzSession, type JazzClient, type AuthProviderSnapshot } from "jazz-tools/svelte";
  import { env } from "$env/dynamic/public";
  import { authClient } from "$lib/auth-client";
  import { setAuthActions } from "$lib/auth-actions";

  let { children: pageChildren }: { children?: Snippet } = $props();
  let jazz = $state.raw<JazzSession<JazzClient>>();
  let auth = $state.raw<AuthProviderSnapshot>({ ready: false, isPending: true });
  let setupError = $state<Error>();
  let connection: ReturnType<typeof connectBetterAuth> | undefined;
  setAuthActions({ signOut: () => connection?.logout() ?? Promise.resolve() });
  onMount(() => {
    let disposed = false;
    let owner: JazzSession<JazzClient> | undefined;
    let unsubscribe: (() => void) | undefined;
    void createJazzSession({ appId: env.PUBLIC_JAZZ_APP_ID!, serverUrl: env.PUBLIC_JAZZ_SERVER_URL! })
      .then(async (session) => {
        owner = session;
        if (disposed) { await owner.close(); return; }
        jazz = session;
        connection = connectBetterAuth(session, authClient);
        auth = connection.getSnapshot();
        unsubscribe = connection.subscribe(() => { auth = connection!.getSnapshot(); });
      }).catch((cause) => { if (!disposed) setupError = cause instanceof Error ? cause : new Error(String(cause)); });
    return () => { disposed = true; unsubscribe?.(); connection?.dispose(); void owner?.close().catch(console.error); };
  });
</script>

{#snippet fallback()}
  {#if setupError || auth.error}
    <p role="alert">{(setupError ?? auth.error)?.message}</p>
    <button onclick={() => setupError ? location.reload() : void connection?.retry()}>Retry</button>
  {:else if auth.isPending}
    <p>Loading...</p>
  {:else}
    {@render pageChildren?.()}
  {/if}
{/snippet}
{#if jazz}
  <JazzSessionProvider session={jazz} {fallback}>
    {#if auth.ready}{@render pageChildren?.()}{:else}{@render fallback()}{/if}
  </JazzSessionProvider>
{:else}
  {@render fallback()}
{/if}
