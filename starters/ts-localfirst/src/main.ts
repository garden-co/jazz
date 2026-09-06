import { createAccountManager, type AccountHandle } from "jazz-tools";
import { createJazzClient, type JazzClientConfig } from "jazz-tools/client";
import { mountApp } from "./app.js";
import { shutdownOnDispose } from "./client-lifecycle.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;
function buildConfig(account: AccountHandle): JazzClientConfig {
  if (!APP_ID || !SERVER_URL) {
    const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }
  return { appId: APP_ID, serverUrl: SERVER_URL, account };
}

async function boot() {
  const root = document.getElementById("root");
  if (!root) throw new Error("#root not found");
  let disposed = false;
  let client: Awaited<ReturnType<typeof createJazzClient>> | undefined;
  const shutdownClients = new WeakSet<Awaited<ReturnType<typeof createJazzClient>>>();
  const gracefulShutdowns = new WeakMap<
    Awaited<ReturnType<typeof createJazzClient>>,
    Promise<void>
  >();
  let restoreQueue = Promise.resolve();
  const dispose = () => {
    disposed = true;
    const active = client;
    client = undefined;
    if (active) shutdownOnDispose(active, gracefulShutdowns, shutdownClients);
  };
  const onPageHide = (event: PageTransitionEvent) => {
    if (event.persisted) return;
    window.removeEventListener("pagehide", onPageHide);
    dispose();
  };
  window.addEventListener("pagehide", onPageHide);

  const accounts = await createAccountManager({ appId: APP_ID!, serverUrl: SERVER_URL! });
  if (disposed) return;
  let account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
  client = await createJazzClient(buildConfig(account));
  if (disposed) {
    if (!shutdownClients.has(client)) {
      shutdownClients.add(client);
      await client.shutdown();
    }
    return;
  }
  const mount = () =>
    mountApp(root, client!.db, account, (secret) => {
      const transition = restoreQueue.then(async () => {
        const active = client;
        if (!active) throw new Error("Jazz client is unavailable");

        // A failed sync barrier leaves the current client and UI usable.
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
        client = undefined;

        let recoveryError: unknown;
        try {
          accounts.restoreLocalFirst(secret);
        } catch (error) {
          recoveryError = error;
        } finally {
          // Reopen even after a rejected restore and for the same account.
          account = accounts.getLoggedIn() ?? account;
          const next = await createJazzClient(buildConfig(account));
          if (disposed) {
            if (!shutdownClients.has(next)) {
              shutdownClients.add(next);
              await next.shutdown();
            }
          } else {
            client = next;
            mount();
          }
        }
        if (recoveryError) throw recoveryError;
      });
      restoreQueue = transition.catch(() => undefined);
      return transition;
    });
  mount();
}

boot();
