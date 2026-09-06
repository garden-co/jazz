<script lang="ts">
  import { onMount } from "svelte";
  import { JazzSvelteClientProvider, createJazzClient, type JazzClient } from "jazz-tools/svelte";
  import { accounts as prepareAccounts, credential } from "$lib/accounts";
  import { authClient } from "$lib/auth-client";
  import { JazzLifecycle, setJazzLifecycle } from "$lib/jazz-lifecycle";
  import { env } from "$env/dynamic/public";
  import type { Snippet } from "svelte";

  let { children: pageChildren }: { children?: Snippet } = $props();
  const appId = env.PUBLIC_JAZZ_APP_ID;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
  let client = $state<JazzClient | undefined>();
  let error = $state<Error | undefined>();
  let providerLinkError = $state<Error | undefined>();
  let lifecycle: JazzLifecycle | undefined;

  setJazzLifecycle({
    transition(action) {
      if (!lifecycle) return Promise.reject(new Error("Jazz lifecycle is not ready"));
      return lifecycle.transition(action);
    },
    reportLinkFailure(cause) {
      providerLinkError = cause instanceof Error ? cause : new Error(String(cause));
    },
  });

  async function retryLink() {
    if (!lifecycle) return;
    try {
      await lifecycle.transition((manager) => manager.linkJWT({ getToken: credential }));
      providerLinkError = undefined;
    } catch (cause) {
      providerLinkError = cause instanceof Error ? cause : new Error(String(cause));
    }
  }

  onMount(() => {
    if (!appId || !serverUrl)
      throw new Error("PUBLIC_JAZZ_APP_ID and PUBLIC_JAZZ_SERVER_URL must be set");
    let cancelled = false;
    void prepareAccounts()
      .then(async (manager) => {
        if (cancelled) return;
        lifecycle = new JazzLifecycle(
          manager,
          (account) => createJazzClient({ appId, serverUrl, account }),
          (next) => {
            if (!cancelled) client = next;
          },
        );
        await lifecycle.attach(async () => {
          const session = await authClient.getSession();
          if (session.data?.session) {
            const retained = manager.getLoggedIn();
            try {
              await manager.loginJWT({ getToken: credential });
            } catch (cause) {
              if (retained?.identity.issuer !== "urn:jazz:local-first") throw cause;
              providerLinkError = cause instanceof Error ? cause : new Error(String(cause));
            }
          } else if (!manager.getLoggedIn()) manager.createLocalFirst();
        });
      })
      .catch((cause) => {
        if (!cancelled) error = cause instanceof Error ? cause : new Error(String(cause));
      });
    return () => {
      cancelled = true;
      if (lifecycle) void lifecycle.close();
    };
  });
</script>

{#if error}
  <p role="alert">{error.message}</p>
{:else if client}
  {#if providerLinkError}
    <aside class="alert-error" role="alert">
      Your signed-in account has not been linked to this local data yet. {providerLinkError.message}
      <button type="button" onclick={retryLink}>Retry linking</button>
    </aside>
  {/if}
  <JazzSvelteClientProvider {client}>
    {#snippet children({ db })}
      {@render pageChildren?.()}
    {/snippet}
    {#snippet fallback()}
      <p>Loading...</p>
    {/snippet}
  </JazzSvelteClientProvider>
{:else}
  <p>Loading...</p>
{/if}
