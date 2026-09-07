<script lang="ts">
  import { onMount } from "svelte";
  import { JazzSessionProvider, createJazzSession, type JazzSession, type JazzSessionSnapshot, type JazzClient } from "jazz-tools/svelte";
  import { env } from "$env/dynamic/public";
  import { credential } from "$lib/accounts";
  import { authClient } from "$lib/auth-client";
  import { setAuthActions } from "$lib/auth-actions";
  import type { Snippet } from "svelte";

  let { children: pageChildren }: { children?: Snippet } = $props();
  const session = authClient.useSession();
  const appId = env.PUBLIC_JAZZ_APP_ID;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
  let setupError = $state<Error | undefined>();
  let recovery = $state<"login" | "register" | "logout">("login");
  let admittedSession = $state<string | null>(null);
  let jazz = $state.raw<JazzSession<JazzClient>>();
  let snapshot = $state.raw<JazzSessionSnapshot<JazzClient>>();
  $effect(() => {
    if (!jazz) return;
    snapshot = jazz.getSnapshot();
    return jazz.subscribe(() => { snapshot = jazz!.getSnapshot(); });
  });
  let ready = $state(false);
  let explicitAuth = $state(false);
  let handledSession: string | null | undefined;
  let observedSession: string | null | undefined;
  let reconcileRequested = false;
  let reconciliation = $state.raw<Promise<void>>();
  let disposed = false;
  let providerRevision = 0;

  // Provider stores can notify after getSession() already returned a newer
  // session. Notifications request reconciliation; only an authoritative read
  // chooses the next account. One in-flight reconciliation coalesces changes.
  async function selectCurrent(owner: JazzSession<JazzClient>) {
    const revision = providerRevision;
    const current = await authClient.getSession();
    if (disposed) return;
    if (revision !== providerRevision) { reconcileRequested = true; return; }
    const key = sessionKey(current.data);
    // A failed explicit signup must remain registration recovery, never an
    // implicit login triggered by the provider finally publishing its session.
    if (setupError && recovery === "register" && key) return;
    if (key === handledSession) return;
    handledSession = key;
    admittedSession = null;
    try {
      if (key) await owner.loginJWT({ getToken: credential });
      else await owner.logout();
      if (!disposed) { setupError = undefined; admittedSession = key; }
    } catch (cause) {
      if (!disposed) { recovery = "login"; setupError = toError(cause); }
    }
  }

  function requestReconciliation() {
    reconcileRequested = true;
    if (!jazz || !ready || explicitAuth || reconciliation || disposed ||
      (recovery === "logout" && setupError)) return;
    const owner = jazz;
    const task = Promise.resolve().then(async () => {
      while (reconcileRequested && !explicitAuth && !disposed) {
        reconcileRequested = false;
        await selectCurrent(owner);
      }
    }).catch((cause) => { if (!disposed) setupError = toError(cause); });
    reconciliation = task;
    void task.finally(() => {
      if (reconciliation === task) reconciliation = undefined;
      if (reconcileRequested && !disposed) requestReconciliation();
    });
  }

  async function beginExplicit() {
    if (!jazz || !ready) throw new Error("Jazz lifecycle is not ready");
    if (explicitAuth) throw new Error("An authentication request is already pending");
    explicitAuth = true;
    await reconciliation;
    if (disposed) throw new Error("Jazz lifecycle is closed");
    return jazz;
  }
  function finishExplicit() {
    explicitAuth = false;
    requestReconciliation();
  }

  async function signOut() {
    const owner = await beginExplicit();
    recovery = "logout";
    try {
      await owner.logout();
      if (disposed) return;
      const result = await authClient.signOut();
      if (result.error) throw new Error(result.error.message ?? "Provider sign-out failed");
      if (disposed) return;
      handledSession = null;
      admittedSession = null;
      setupError = undefined;
    } catch (cause) {
      if (!disposed) setupError = toError(cause);
      throw cause;
    } finally { finishExplicit(); }
  }

  setAuthActions({
    async authenticate(enroll, request) {
      const owner = await beginExplicit();
      let authenticated = false;
      try {
        const result = await request();
        if (result.error) throw new Error(result.error.message ?? (enroll ? "Sign-up failed" : "Sign-in failed"));
        authenticated = true;
        if (disposed) return;
        const current = await authClient.getSession();
        if (disposed) return;
        const key = sessionKey(current.data);
        if (!key) throw new Error("Provider did not establish a session");
        handledSession = key;
        admittedSession = null;
        if (enroll) await owner.registerJWT({ getToken: credential });
        else await owner.loginJWT({ getToken: credential });
        if (disposed) return;
        setupError = undefined;
        admittedSession = key;
      } catch (cause) {
        if (authenticated && !disposed) { recovery = enroll ? "register" : "login"; setupError = toError(cause); }
        throw cause;
      } finally { finishExplicit(); }
    },
    signOut,
    reportFailure(cause) { recovery = "logout"; setupError = toError(cause); },
  });

  $effect(() => {
    const key = sessionKey($session.data);
    if (ready && key !== observedSession) {
      observedSession = key;
      providerRevision++;
      requestReconciliation();
    }
  });

  async function recover() {
    if (recovery === "logout") { await signOut().catch(() => {}); return; }
    let owner: JazzSession<JazzClient>;
    try { owner = await beginExplicit(); }
    catch (cause) { if (!disposed) setupError ??= toError(cause); return; }
    try {
      const current = await authClient.getSession();
      if (disposed) return;
      const key = sessionKey(current.data);
      if (!key) throw new Error("Provider did not establish a session");
      handledSession = key;
      if (owner.getSnapshot().status === "error" &&
        owner.getSnapshot().account?.identity.subject === current.data?.user.id &&
        owner.getSnapshot().account?.identity.issuer !== "urn:jazz:local-first") await owner.retry();
      else if (recovery === "login") await owner.loginJWT({ getToken: credential });
      else await owner.registerJWT({ getToken: credential });
      if (disposed) return;
      setupError = undefined;
      admittedSession = key;
    } catch (cause) { if (!disposed) setupError = toError(cause); }
    finally { finishExplicit(); }
  }

  onMount(() => {
    if (!appId || !serverUrl) throw new Error("PUBLIC_JAZZ_APP_ID and PUBLIC_JAZZ_SERVER_URL must be set");
    let owner: JazzSession<JazzClient> | undefined;
    void (async () => {
      owner = await createJazzSession({ appId, serverUrl });
      if (disposed) { await owner.close(); return; }
      await selectCurrent(owner);
      if (disposed) await owner.close();
      else { jazz = owner; ready = true; }
    })().catch((cause) => { if (!disposed) setupError = toError(cause); });
    return () => {
      disposed = true;
      if (owner) void owner.close().catch((cause) => console.error("Jazz client shutdown failed", cause));
    };
  });

  function sessionKey(value: { session?: { id?: string } | null; user?: { id?: string } | null } | null | undefined) {
    return value?.session?.id ?? value?.user?.id ?? null;
  }
  function toError(cause: unknown) { return cause instanceof Error ? cause : new Error(String(cause)); }
</script>

{#if jazz}
  <JazzSessionProvider session={jazz}>
    {#snippet children()}
      {#if admittedSession === sessionKey($session.data) && snapshot?.account?.identity.subject === $session.data?.user.id && snapshot?.account?.identity.issuer !== "urn:jazz:local-first"}
        {#if setupError}<aside class="alert-error" role="alert">{setupError.message}</aside>{/if}
        {@render pageChildren?.()}
      {:else if setupError}
        <p role="alert">{setupError.message}</p><button onclick={recover} disabled={explicitAuth || reconciliation !== undefined}>{recovery === "logout" ? "Retry sign out" : "Retry"}</button>
      {:else}<p>Loading...</p>{/if}
    {/snippet}
    {#snippet fallback()}
      {#if setupError && $session.data?.session}
        <main class="page-center"><div class="card"><p class="alert-error" role="alert">{setupError.message}</p><button type="button" class="btn-primary" onclick={recover} disabled={explicitAuth || reconciliation !== undefined}>{recovery === "logout" ? "Retry sign out" : recovery === "login" ? "Retry sign in" : "Complete account setup"}</button></div></main>
      {:else if !$session.data?.session}{@render pageChildren?.()}
      {:else}<p>Loading...</p>{/if}
    {/snippet}
  </JazzSessionProvider>
{:else if setupError}<p role="alert">{setupError.message}</p>
{:else}<p>Loading...</p>{/if}
