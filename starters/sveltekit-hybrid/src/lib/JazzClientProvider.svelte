<script lang="ts">
  import { onMount } from "svelte";
  import {
    createJazzClient,
    JazzSvelteProvider,
    BrowserAuthSecretStore,
  } from "jazz-tools/svelte";
  import type { DbConfig } from "jazz-tools";
  import type { Snippet } from "svelte";
  import { env } from "$env/dynamic/public";
  import { authClient, getToken } from "$lib/auth-client";

  let {
    authenticated,
    children: pageChildren,
  }: { authenticated: boolean; children: Snippet } = $props();

  let client = $state<ReturnType<typeof createJazzClient> | null>(null);

  onMount(() => {
    let unsubRefresh: (() => void) | undefined;

    (async () => {
      const config = await buildConfig(authenticated);
      if (!config) return;
      const jazzClient = createJazzClient(config);
      client = jazzClient;

      if (authenticated) {
        const resolved = await jazzClient;
        unsubRefresh = resolved.db.onAuthChanged(async (state) => {
          if (state.status !== "unauthenticated") return;
          const fresh = await getToken();
          if (fresh) resolved.db.updateAuthToken(fresh);
        });
      }
    })();

    return () => unsubRefresh?.();
  });

  async function buildConfig(auth: boolean): Promise<DbConfig | null> {
    const appId = env.PUBLIC_JAZZ_APP_ID;
    const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
    if (!appId || !serverUrl) {
      const missing = [
        !appId && "PUBLIC_JAZZ_APP_ID",
        !serverUrl && "PUBLIC_JAZZ_SERVER_URL",
      ]
        .filter((v) => !!v)
        .join(" & ");
      console.error(
        `${missing} not set — the jazzSvelteKit() plugin should inject these.`,
      );
      return Promise.resolve(null);
    }
    const base: Omit<DbConfig, "jwtToken" | "secret"> = { appId, serverUrl };

    if (auth) {
      return getToken().then((token) =>
        token ? { ...base, jwtToken: token } : null,
      );
    }

    return BrowserAuthSecretStore.getOrCreateSecret().then((secret) => ({
      ...base,
      secret,
    }));
  }
</script>

{#if client}
  <JazzSvelteProvider {client}>
    {#snippet children()}
      {@render pageChildren?.()}
    {/snippet}
    {#snippet fallback()}
      <p>Loading...</p>
    {/snippet}
  </JazzSvelteProvider>
{/if}
