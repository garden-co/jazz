import { createContext, StrictMode, useContext, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { createJazzClient, JazzClientProvider } from "jazz-tools/react";
import { createAccountManager } from "jazz-tools";
import { accounts as prepareAccounts } from "./accounts";
import { authClient } from "./auth-client";
import { getToken } from "./accounts";
import { JazzLifecycle } from "./jazz-lifecycle";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;
interface JazzLifecycleApi {
  transition: JazzLifecycle["transition"];
  reportLinkFailure(cause: unknown): void;
}

const JazzLifecycleContext = createContext<JazzLifecycleApi | null>(null);

export function useJazzLifecycle(): JazzLifecycleApi {
  const lifecycle = useContext(JazzLifecycleContext);
  if (!lifecycle) throw new Error("Jazz lifecycle is not ready");
  return lifecycle;
}

function HybridProvider({ children }: React.PropsWithChildren) {
  const [accounts, setAccounts] = useState<Awaited<ReturnType<typeof createAccountManager>>>();
  useEffect(() => {
    if (!APP_ID || !SERVER_URL)
      throw new Error("VITE_JAZZ_APP_ID and VITE_JAZZ_SERVER_URL must be set");
    let cancelled = false;
    void prepareAccounts().then((manager) => {
      if (!cancelled) setAccounts(manager);
    });
    return () => {
      cancelled = true;
    };
  }, []);
  if (!accounts || !APP_ID || !SERVER_URL) return <p>Loading...</p>;
  return (
    <AccountContext accounts={accounts} appId={APP_ID} serverUrl={SERVER_URL}>
      {children}
    </AccountContext>
  );
}

function AccountContext({
  accounts,
  appId,
  serverUrl,
  children,
}: React.PropsWithChildren<{
  accounts: Awaited<ReturnType<typeof createAccountManager>>;
  appId: string;
  serverUrl: string;
}>) {
  const [client, setClient] = useState<Awaited<ReturnType<typeof createJazzClient>>>();
  const [error, setError] = useState<Error>();
  const [providerLinkError, setProviderLinkError] = useState<Error>();
  const lifecycleRef = useRef<JazzLifecycle | undefined>(undefined);
  if (!lifecycleRef.current) {
    lifecycleRef.current = new JazzLifecycle(
      accounts,
      (account) => createJazzClient({ appId, serverUrl, account }),
      setClient,
    );
  }
  const lifecycle = lifecycleRef.current;
  const lifecycleApi = useMemo<JazzLifecycleApi>(
    () => ({
      transition: lifecycle.transition.bind(lifecycle),
      reportLinkFailure(cause) {
        setProviderLinkError(cause instanceof Error ? cause : new Error(String(cause)));
      },
    }),
    [lifecycle],
  );

  async function retryLink() {
    try {
      await lifecycle.transition((manager) => manager.linkJWT({ getToken }));
      setProviderLinkError(undefined);
    } catch (cause) {
      setProviderLinkError(cause instanceof Error ? cause : new Error(String(cause)));
    }
  }

  useEffect(() => {
    void lifecycle
      .attach(async () => {
        const session = await authClient.getSession();
        if (session.data?.session) {
          const retained = accounts.getLoggedIn();
          try {
            await accounts.loginJWT({ getToken });
          } catch (cause) {
            if (retained?.identity.issuer !== "urn:jazz:local-first") throw cause;
            setProviderLinkError(cause instanceof Error ? cause : new Error(String(cause)));
          }
        } else if (!accounts.getLoggedIn()) accounts.createLocalFirst();
      })
      .catch((cause) => setError(cause instanceof Error ? cause : new Error(String(cause))));
    return () => {
      void lifecycle.close();
    };
  }, [accounts, lifecycle]);

  if (error) return <p role="alert">{error.message}</p>;
  if (!client) return <p>Loading...</p>;

  return (
    <JazzLifecycleContext.Provider value={lifecycleApi}>
      {providerLinkError && (
        <aside className="alert-error" role="alert">
          Your signed-in account has not been linked to this local data yet.{" "}
          {providerLinkError.message}
          <button type="button" onClick={retryLink}>
            Retry linking
          </button>
        </aside>
      )}
      <JazzClientProvider client={client}>{children}</JazzClientProvider>
    </JazzLifecycleContext.Provider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <HybridProvider>
      <App />
    </HybridProvider>
  </StrictMode>,
);
