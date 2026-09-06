import { StrictMode, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { createAccountManager, type AccountHandle } from "jazz-tools";
import { createJazzClient } from "jazz-tools/client";
import { JazzClientProvider } from "jazz-tools/react";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

if (!APP_ID || !SERVER_URL) {
  const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
    .filter((v) => !!v)
    .join(" & ");
  throw new Error(
    `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
  );
}

type Accounts = Awaited<ReturnType<typeof createAccountManager>>;
type Deferred<T> = { promise: Promise<T>; resolve(value: T): void; reject(reason: unknown): void };

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function AccountRuntime({
  accounts,
  initialAccount,
}: {
  accounts: Accounts;
  initialAccount: AccountHandle;
}) {
  const [runtime, setRuntime] = useState({ account: initialAccount, generation: 0 });
  const [client, setClient] = useState<Awaited<ReturnType<typeof createJazzClient>> | undefined>(
    undefined,
  );
  const [clientError, setClientError] = useState<Error | undefined>(undefined);
  const activeClient = useRef<Awaited<ReturnType<typeof createJazzClient>> | undefined>(undefined);
  const shutdownClients = useRef(new WeakSet<Awaited<ReturnType<typeof createJazzClient>>>());
  const gracefulShutdowns = useRef(
    new WeakMap<Awaited<ReturnType<typeof createJazzClient>>, Promise<void>>(),
  );
  const restoreQueue = useRef<Promise<void>>(Promise.resolve());
  const replacementClient = useRef<
    Deferred<Awaited<ReturnType<typeof createJazzClient>>> | undefined
  >(undefined);

  useEffect(
    () => () => {
      replacementClient.current?.reject(
        new Error("Jazz client closed before replacement finished"),
      );
    },
    [],
  );

  useEffect(() => {
    let cancelled = false;
    const pendingReplacement = replacementClient.current;
    setClientError(undefined);
    const next = createJazzClient({
      appId: APP_ID!,
      serverUrl: SERVER_URL!,
      account: runtime.account,
    });
    void next.then(
      (opened) => {
        if (cancelled) {
          return;
        }
        activeClient.current = opened;
        setClient(opened);
        pendingReplacement?.resolve(opened);
      },
      (reason) => {
        if (!cancelled) {
          pendingReplacement?.reject(reason);
          setClientError(reason instanceof Error ? reason : new Error(String(reason)));
        }
      },
    );
    return () => {
      cancelled = true;
      if (activeClient.current) activeClient.current = undefined;
      setClient(undefined);
      void next
        .then((opened) => {
          const graceful = gracefulShutdowns.current.get(opened);
          if (graceful) {
            void graceful.catch(() => opened.shutdown()).catch(() => undefined);
            return;
          }
          if (shutdownClients.current.has(opened)) return;
          shutdownClients.current.add(opened);
          return opened.shutdown();
        })
        .catch(() => undefined);
    };
  }, [runtime]);

  function restore(secret: string): Promise<void> {
    const transition = restoreQueue.current.then(async () => {
      const active = activeClient.current;
      if (!active) throw new Error("Jazz client is still starting");

      // If this sync barrier rejects, leave the existing client in place.
      shutdownClients.current.add(active);
      const graceful = active.shutdown({ waitForSync: true });
      gracefulShutdowns.current.set(active, graceful);
      try {
        await graceful;
      } catch (error) {
        shutdownClients.current.delete(active);
        gracefulShutdowns.current.delete(active);
        throw error;
      }
      gracefulShutdowns.current.delete(active);
      activeClient.current = undefined;
      setClient(undefined);

      let recoveryError: unknown;
      try {
        accounts.restoreLocalFirst(secret);
      } catch (error) {
        recoveryError = error;
      } finally {
        // Always replace a successfully closed client, including same-account
        // recovery and a rejected recovery phrase.
        const selected = accounts.getLoggedIn() ?? runtime.account;
        const nextClient = deferred<Awaited<ReturnType<typeof createJazzClient>>>();
        replacementClient.current = nextClient;
        setRuntime((current) => ({ account: selected, generation: current.generation + 1 }));
        await nextClient.promise;
      }
      if (recoveryError) throw recoveryError;
    });
    restoreQueue.current = transition.catch(() => undefined);
    return transition;
  }
  if (clientError) throw clientError;
  if (!client) return <p>Loading...</p>;
  return (
    <JazzClientProvider client={client}>
      <App account={runtime.account} onRestore={restore} />
    </JazzClientProvider>
  );
}

function Root() {
  const [accounts, setAccounts] = useState<Accounts>();
  const [error, setError] = useState<Error>();
  useEffect(() => {
    let cancelled = false;
    void createAccountManager({ appId: APP_ID!, serverUrl: SERVER_URL! })
      .then((manager) => {
        if (cancelled) return;
        if (!manager.getLoggedIn()) manager.createLocalFirst();
        setAccounts(manager);
      })
      .catch(
        (reason) =>
          !cancelled && setError(reason instanceof Error ? reason : new Error(String(reason))),
      );
    return () => {
      cancelled = true;
    };
  }, []);
  if (error) throw error;
  return accounts ? (
    <AccountRuntime accounts={accounts} initialAccount={accounts.getLoggedIn()!} />
  ) : (
    <p>Loading...</p>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Root />
  </StrictMode>,
);
