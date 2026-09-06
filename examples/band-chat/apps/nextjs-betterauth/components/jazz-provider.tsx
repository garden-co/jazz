"use client";

import { createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import { createJazzClient, JazzClientProvider, type JazzClient } from "jazz-tools/react";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";
import { prepareAccounts } from "@/src/lib/accounts";
import { JazzLifecycle } from "@/src/lib/jazz-lifecycle";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
const registerIntentKey = "band-chat-register-jwt";
type Accounts = Awaited<ReturnType<typeof prepareAccounts>>;
type LifecycleApi = { signOut(): Promise<void> };
const LifecycleContext = createContext<LifecycleApi | null>(null);

export function useBandChatLifecycle(): LifecycleApi {
  const lifecycle = useContext(LifecycleContext);
  if (!lifecycle) throw new Error("BandChat Jazz lifecycle is not ready.");
  return lifecycle;
}

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session, isPending } = authClient.useSession();
  const [accounts, setAccounts] = useState<Accounts>();
  const [startupError, setStartupError] = useState<Error>();
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!appId || !serverUrl) throw new Error("withJazz must provide the public app configuration");
    let cancelled = false;
    void prepareAccounts(appId, serverUrl).then(
      (prepared) => !cancelled && setAccounts(prepared),
      (cause) =>
        !cancelled && setStartupError(cause instanceof Error ? cause : new Error(String(cause))),
    );
    return () => {
      cancelled = true;
    };
  }, [attempt]);
  if (startupError)
    return (
      <section>
        <p role="alert">Could not start BandChat: {startupError.message}</p>
        <button
          onClick={() => {
            setStartupError(undefined);
            setAttempt((value) => value + 1);
          }}
        >
          Retry
        </button>
      </section>
    );
  if (isPending || !session?.user || !accounts)
    return <p className="loading-state">Connecting BandChat…</p>;
  return (
    <AccountContext accounts={accounts} principal={session.user.id} sessionId={session.session.id}>
      {children}
    </AccountContext>
  );
}

function AccountContext({
  accounts,
  principal,
  sessionId,
  children,
}: {
  accounts: Accounts;
  principal: string;
  sessionId: string;
  children: React.ReactNode;
}) {
  const [client, setClient] = useState<JazzClient>();
  const [error, setError] = useState<Error>();
  const lifecycleRef = useRef<JazzLifecycle | undefined>(undefined);
  if (!lifecycleRef.current)
    lifecycleRef.current = new JazzLifecycle(
      accounts,
      (account) => createJazzClient({ appId, serverUrl, account }),
      setClient,
    );
  const lifecycle = lifecycleRef.current;
  const registering = sessionStorage.getItem(registerIntentKey) === "1";
  const enroll = useMemo(
    () =>
      registering
        ? (manager: Accounts) => manager.registerJWT({ getToken: requireBetterAuthToken })
        : (manager: Accounts) => manager.loginJWT({ getToken: requireBetterAuthToken }),
    [registering],
  );

  useEffect(() => {
    let active = true;
    void lifecycle.reconcile(principal, sessionId, enroll).then(
      () => {
        if (active) {
          setError(undefined);
          sessionStorage.removeItem(registerIntentKey);
        }
      },
      (cause) => active && setError(cause instanceof Error ? cause : new Error(String(cause))),
    );
    return () => {
      active = false;
    };
  }, [enroll, lifecycle, principal, sessionId]);
  useEffect(
    () => () => {
      void lifecycle
        .close()
        .catch((cause) => console.error("BandChat Jazz shutdown failed", cause));
    },
    [lifecycle],
  );

  const lifecycleApi = useMemo<LifecycleApi>(
    () => ({
      async signOut() {
        try {
          await lifecycle.transition(async (manager) => {
            await authClient.signOut();
            manager.logout();
          });
          window.location.assign("/");
        } catch (cause) {
          setError(cause instanceof Error ? cause : new Error(String(cause)));
        }
      },
    }),
    [lifecycle],
  );

  const visibleClient = client && lifecycle.isCurrent(principal, sessionId) ? client : undefined;
  if (error && !visibleClient)
    return (
      <section>
        <p role="alert">Could not connect BandChat: {error.message}</p>
        <button
          onClick={() =>
            void lifecycle.reconcile(principal, sessionId, enroll).then(
              () => setError(undefined),
              (cause) => setError(cause instanceof Error ? cause : new Error(String(cause))),
            )
          }
        >
          Retry
        </button>
      </section>
    );
  if (!visibleClient) return <p className="loading-state">Connecting BandChat…</p>;
  return (
    <LifecycleContext.Provider value={lifecycleApi}>
      {error && <p role="alert">Could not update BandChat: {error.message}</p>}
      <JazzClientProvider client={visibleClient}>{children}</JazzClientProvider>
    </LifecycleContext.Provider>
  );
}

async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a Jazz session token.");
  return token;
}
