<script lang="ts">
  import { onMount } from "svelte";
  import { JazzSessionProvider, createJazzSession, type JazzSession, type JazzClient } from "jazz-tools/svelte";
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
  let recovery = $state<"login" | "register">("register");
  let admittedSession = $state<string | null>(null);
  let jazz = $state.raw<JazzSession<JazzClient>>();
  let ready = false;
  let explicitAuth = false;
  let version = 0;
  let handledSession: string | null | undefined;

  setAuthActions({
    async authenticate(enroll, request) {
      if (!jazz) throw new Error("Jazz lifecycle is not ready");
      explicitAuth = true;
      const currentVersion = ++version;
      try {
        const result = await request();
        if (result.error) throw new Error(result.error.message ?? (enroll ? "Sign-up failed" : "Sign-in failed"));
        const current = await authClient.getSession();
        handledSession = sessionKey(current.data);
        if (currentVersion !== version) return;
        if (enroll) await jazz.registerJWT({ getToken: credential });
        else await jazz.loginJWT({ getToken: credential });
        setupError = undefined;
        admittedSession = handledSession;
      } catch (cause) {
        const error = toError(cause);
        if (sessionKey((await authClient.getSession()).data)) { recovery = enroll ? "register" : "login"; setupError = error; }
        throw error;
      } finally {
        explicitAuth = false;
        reconcile(sessionKey($session.data));
      }
    },
    reportFailure(cause) { setupError = toError(cause); },
  });

  $effect(() => {
    const key = sessionKey($session.data);
    if (ready) reconcile(key);
  });

  function reconcile(key: string | null) {
    if (!jazz || explicitAuth || key === handledSession) return;
    handledSession = key;
    admittedSession = null;
    const currentVersion = ++version;
    void (key ? jazz.loginJWT({ getToken: credential }) : jazz.logout()).then(() => {
      if (currentVersion === version) { setupError = undefined; admittedSession = key; }
    }).catch((cause) => {
      if (currentVersion === version) { recovery = "login"; setupError = toError(cause); }
    });
  }

  function recover() {
    if (!jazz) return;
    const currentVersion = ++version;
    const recoveryKey = sessionKey($session.data);
    void (recovery === "login" ? jazz.loginJWT({ getToken: credential }) : jazz.registerJWT({ getToken: credential }))
      .then(() => {
        if (currentVersion === version && recoveryKey === sessionKey($session.data)) {
          setupError = undefined;
          admittedSession = recoveryKey;
        }
      })
      .catch((cause) => {
        if (currentVersion === version && recoveryKey === sessionKey($session.data)) setupError = toError(cause);
      });
  }

  onMount(() => {
    if (!appId || !serverUrl) throw new Error("PUBLIC_JAZZ_APP_ID and PUBLIC_JAZZ_SERVER_URL must be set");
    let cancelled = false;
    let owner: JazzSession<JazzClient> | undefined;
    void (async () => {
      owner = await createJazzSession({ appId, serverUrl });
      if (cancelled) { await owner.close(); return; }
      const current = await authClient.getSession();
      const key = sessionKey(current.data);
      handledSession = key;
      const currentVersion = ++version;
      try {
        if (key) await owner.loginJWT({ getToken: credential });
        else await owner.logout();
        if (!cancelled && currentVersion === version) admittedSession = key;
      } catch (cause) {
        if (!cancelled && currentVersion === version) { recovery = "login"; setupError = toError(cause); }
      }
      if (cancelled) await owner.close();
      else { jazz = owner; ready = true; }
    })().catch((cause) => { if (!cancelled) setupError = toError(cause); });
    return () => {
      cancelled = true;
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
      {#if admittedSession === sessionKey($session.data)}
        {#if setupError}<aside class="alert-error" role="alert">{setupError.message}</aside>{/if}
        {@render pageChildren?.()}
      {:else if setupError}
        <p role="alert">{setupError.message}</p><button onclick={recover}>Retry sign in</button>
      {:else}<p>Loading...</p>{/if}
    {/snippet}
    {#snippet fallback()}
      {#if setupError && $session.data?.session}
        <main class="page-center"><div class="card"><p class="alert-error" role="alert">{setupError.message}</p><button type="button" class="btn-primary" onclick={recover}>{recovery === "login" ? "Retry sign in" : "Complete account setup"}</button></div></main>
      {:else if !$session.data?.session}{@render pageChildren?.()}
      {:else}<p>Loading...</p>{/if}
    {/snippet}
  </JazzSessionProvider>
{:else if setupError}<p role="alert">{setupError.message}</p>
{:else}<p>Loading...</p>{/if}
