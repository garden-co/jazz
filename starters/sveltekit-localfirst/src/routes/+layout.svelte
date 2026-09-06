<script lang="ts">
  import "../app.css";
  import { onMount } from "svelte";
  import { JazzSvelteClientProvider, createJazzClient, type JazzClient } from "jazz-tools/svelte";
  import { createAccountManager, type AccountHandle } from "jazz-tools";
  import { env } from "$env/dynamic/public";
  import AuthBackup from "$lib/AuthBackup.svelte";

  let { children: pageChildren } = $props();

  const appId = env.PUBLIC_JAZZ_APP_ID;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
  let account = $state<AccountHandle>();
  let client = $state<JazzClient>();
  let error = $state<Error>();
  let restore = $state<((secret: string) => Promise<void>)>();

  $effect(() => {
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
    }
  });

  onMount(() => {
    if (!appId || !serverUrl) return;
    let cancelled = false;
    let activeClient: JazzClient | undefined;
    const shutdownClients = new WeakSet<JazzClient>();
    const gracefulShutdowns = new WeakMap<JazzClient, Promise<void>>();
    let restoreQueue = Promise.resolve();

    const openClient = async (selected: AccountHandle) => {
      const next = await createJazzClient({ appId, serverUrl, account: selected });
      if (cancelled) {
        await next.shutdown();
        return;
      }
      activeClient = next;
      client = next;
    };

    void (async () => {
      const manager = await createAccountManager({ appId, serverUrl });
      if (cancelled) return;

      account = manager.getLoggedIn() ?? manager.createLocalFirst();
      await openClient(account);
      if (cancelled) return;

      restore = (secret) => {
        const transition = restoreQueue.then(async () => {
          const active = activeClient;
          if (!active) throw new Error("Jazz client is unavailable");

          // If this sync barrier fails, retain the old client and account.
          shutdownClients.add(active);
          const graceful = active.shutdown({ waitForSync: true });
          gracefulShutdowns.set(active, graceful);
          try {
            await graceful;
          } catch (error) {
            shutdownClients.delete(active);
            gracefulShutdowns.delete(active);
            throw error;
          }
          gracefulShutdowns.delete(active);
          activeClient = undefined;
          client = undefined;

          let recoveryError: unknown;
          try {
            manager.restoreLocalFirst(secret);
          } catch (reason) {
            recoveryError = reason;
          } finally {
            // Recovery runs outside the old context. Reopen even when the
            // secret describes the current account or recovery rejects.
            const selected = manager.getLoggedIn() ?? account;
            if (!selected) throw new Error("Jazz account is unavailable");
            account = selected;
            await openClient(selected);
          }
          if (recoveryError) throw recoveryError;
        });
        restoreQueue = transition.catch(() => undefined);
        return transition;
      };
    })().catch((reason) => {
      if (!cancelled) error = reason instanceof Error ? reason : new Error(String(reason));
    });

    return () => {
      cancelled = true;
      restore = undefined;
      const active = activeClient;
      activeClient = undefined;
      client = undefined;
      if (active) {
        const graceful = gracefulShutdowns.get(active);
        if (graceful) {
          void graceful.catch(() => active.shutdown()).catch(() => undefined);
        } else if (!shutdownClients.has(active)) {
          shutdownClients.add(active);
          void active.shutdown();
        }
      }
    };
  });

</script>

{#if error}
  {@const _ = (() => { throw error; })()}
{:else if client}
  <JazzSvelteClientProvider {client}>
    {#snippet children()}
      <main class="dashboard">
        <header>
          <img src="/jazz.svg" alt="Jazz" class="wordmark" />
        </header>
        {@render pageChildren?.()}
        {#if account && restore}
          <AuthBackup {account} onRestore={restore} />
        {/if}
      </main>
    {/snippet}
    {#snippet fallback()}
      <p>Loading...</p>
    {/snippet}
  </JazzSvelteClientProvider>
{:else}
  <p>Loading...</p>
{/if}
