<script lang="ts">
  import { onMount, type Snippet } from "svelte";
  import { JazzSessionProvider, createJazzSession, type JazzSession, type JazzClient } from "jazz-tools/svelte";
  import { credential } from "$lib/accounts";
  import { authClient } from "$lib/auth-client";
  import { env } from "$env/dynamic/public";
  import { setAuthActions } from "$lib/auth-actions";
  import AccountStatus from "$lib/AccountStatus.svelte";
  let { children: pageChildren }: { children?: Snippet } = $props();
  let jazz = $state.raw<JazzSession<JazzClient>>();
  let error = $state<Error>();
  let authError = $state<Error>();
  async function signOut() {
    if (!jazz) return;
    try {
      await jazz.logout();
      const result = await authClient.signOut();
      if (result.error) throw new Error(result.error.message ?? "Provider sign-out failed");
      await jazz.createLocalFirst();
      authError = undefined;
    } catch (cause) {
      authError = cause instanceof Error ? cause : new Error(String(cause));
      throw cause;
    }
  }
  setAuthActions({ signOut });
  onMount(() => {
    const appId = env.PUBLIC_JAZZ_APP_ID;
    const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
    if (!appId || !serverUrl) throw new Error("PUBLIC_JAZZ_APP_ID and PUBLIC_JAZZ_SERVER_URL must be set");
    let cancelled = false;
    let owner: JazzSession<JazzClient> | undefined;
    void (async () => {
      owner = await createJazzSession({ appId, serverUrl, initial: "local-first" });
      if (cancelled) { await owner.close(); return; }
      const auth = await authClient.getSession();
      if (auth.data?.session) {
        try { await owner.loginJWT({ getToken: credential }); }
        catch (cause) { if (owner.getSnapshot().account?.identity.issuer !== "urn:jazz:local-first") throw cause; }
      }
      if (cancelled) await owner.close();
      else jazz = owner;
    })().catch((cause) => { if (!cancelled) error = cause instanceof Error ? cause : new Error(String(cause)); });
    return () => { cancelled = true; if (owner) void owner.close().catch(console.error); };
  });
</script>

{#if error}<p role="alert">{error.message}</p>
{:else if jazz}
  {#if authError}<p role="alert">{authError.message}</p><button onclick={() => signOut().catch(() => {})}>Retry sign out</button>{/if}
  <JazzSessionProvider session={jazz}>
    {#snippet children()}<AccountStatus />{@render pageChildren?.()}{/snippet}
    {#snippet fallback()}<AccountStatus />{/snippet}
  </JazzSessionProvider>
{:else}<p>Loading...</p>{/if}
