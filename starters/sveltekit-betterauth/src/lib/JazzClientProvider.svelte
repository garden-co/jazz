<script lang="ts">
  import { onMount } from "svelte";
  import { JazzSvelteClientProvider, createJazzClient, type JazzClient } from "jazz-tools/svelte";
  import { env } from "$env/dynamic/public";
  import { accounts as prepareAccounts, credential } from "$lib/accounts";
  import { authClient } from "$lib/auth-client";
  import { JazzLifecycle, setJazzLifecycle } from "$lib/jazz-lifecycle";
  import type { Snippet } from "svelte";

  let { children: pageChildren }: { children?: Snippet } = $props();
  const session = authClient.useSession();
  const appId = env.PUBLIC_JAZZ_APP_ID;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
  let client = $state<JazzClient | undefined>();
  let setupError = $state<Error | undefined>();
  let recovery = $state<"login" | "register">("register");
  let admittedSession = $state<string | null>(null);
  let lifecycle = $state<JazzLifecycle | undefined>();
  let ready = false;
  let explicitAuth = false;
  let version = 0;
  let handledSession: string | null | undefined;

  setJazzLifecycle({
    transition(action, isCurrent) {
      if (!lifecycle) return Promise.reject(new Error("Jazz lifecycle is not ready"));
      return lifecycle.transition(action, isCurrent);
    },
    async authenticate(enroll, request) {
      if (!lifecycle) throw new Error("Jazz lifecycle is not ready");
      explicitAuth = true;
      const currentVersion = ++version;
      try {
        const result = await request();
        if (result.error) throw new Error(result.error.message ?? (enroll ? "Sign-up failed" : "Sign-in failed"));
        const current = await authClient.getSession();
        handledSession = sessionKey(current.data);
        await lifecycle.transition(
          (manager) => enroll ? manager.registerJWT({ getToken: credential }) : manager.loginJWT({ getToken: credential }),
          () => explicitAuth && currentVersion === version,
        );
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
    if (!lifecycle || explicitAuth || key === handledSession) return;
    handledSession = key;
    admittedSession = null;
    const currentVersion = ++version;
    void lifecycle.transition(
      async (manager) => { if (key) await manager.loginJWT({ getToken: credential }); else manager.logout(); },
      () => !explicitAuth && currentVersion === version,
      false,
    ).then(() => {
      if (currentVersion === version) { setupError = undefined; admittedSession = key; }
    }).catch((cause) => {
      if (currentVersion === version) { recovery = "login"; setupError = toError(cause); }
    });
  }

  function recover() {
    if (!lifecycle) return;
    const currentVersion = ++version;
    const recoveryKey = sessionKey($session.data);
    void lifecycle.transition(
      (manager) => recovery === "login" ? manager.loginJWT({ getToken: credential }) : manager.registerJWT({ getToken: credential }),
      () => currentVersion === version && recoveryKey === sessionKey($session.data),
      recovery !== "login",
    )
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
    void prepareAccounts().then(async (manager) => {
      if (cancelled) return;
      const next = new JazzLifecycle(manager, (account) => createJazzClient({ appId, serverUrl, account }), (value) => { if (!cancelled) client = value; });
      lifecycle = next;
      const current = await authClient.getSession();
      const key = sessionKey(current.data);
      handledSession = key;
      const currentVersion = ++version;
      try {
        await next.attach(async () => { if (key) await manager.loginJWT({ getToken: credential }); });
        if (!cancelled && currentVersion === version) admittedSession = key;
      } catch (cause) {
        if (!cancelled && currentVersion === version) { recovery = "login"; setupError = toError(cause); }
      } finally {
        if (!cancelled) ready = true;
      }
    }).catch((cause) => { if (!cancelled) setupError = toError(cause); });
    return () => {
      cancelled = true;
      if (lifecycle) void lifecycle.close().catch((cause) => console.error("Jazz client shutdown failed", cause));
    };
  });

  function sessionKey(value: { session?: { id?: string } | null; user?: { id?: string } | null } | null | undefined) {
    return value?.session?.id ?? value?.user?.id ?? null;
  }
  function toError(cause: unknown) { return cause instanceof Error ? cause : new Error(String(cause)); }
</script>

{#if client && admittedSession === sessionKey($session.data)}
  <JazzSvelteClientProvider {client}>
    {#snippet children({ db })}
      {#if setupError}<aside class="alert-error" role="alert">{setupError.message}</aside>{/if}
      {@render pageChildren?.()}
    {/snippet}
    {#snippet fallback()}<p>Loading...</p>{/snippet}
  </JazzSvelteClientProvider>
{:else if setupError && $session.data?.session}
  <main class="page-center"><div class="card"><p class="alert-error" role="alert">{setupError.message}</p><button type="button" class="btn-primary" onclick={recover}>{recovery === "login" ? "Retry sign in" : "Complete account setup"}</button></div></main>
{:else}
  {@render pageChildren?.()}
{/if}
