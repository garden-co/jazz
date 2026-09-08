"use client";

import { createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import { createJazzClient, JazzClientProvider, type JazzClient } from "jazz-tools/react";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";
import { prepareAccounts } from "../src/lib/accounts";
import { JazzLifecycle } from "../src/lib/jazz-lifecycle";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
const registerIntentKey = "record-player-register-jwt";
type Accounts = Awaited<ReturnType<typeof prepareAccounts>>;
const LifecycleContext = createContext<JazzLifecycle | null>(null);

export function useRecordPlayerLifecycle(): JazzLifecycle {
  const lifecycle = useContext(LifecycleContext);
  if (!lifecycle) throw new Error("RecordPlayer Jazz lifecycle is not ready.");
  return lifecycle;
}

export function RecordPlayerProvider({ children }: { children: React.ReactNode }) {
  const { data: session, isPending } = authClient.useSession();
  const [accounts, setAccounts] = useState<Accounts | null>(null);
  const [startupError, setStartupError] = useState<Error>();
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
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
        <p role="alert">Could not start RecordPlayer: {startupError.message}</p>
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
  if (isPending) return <p>Preparing your RecordPlayer…</p>;
  if (!session?.user) return <SignIn />;
  if (!accounts) return <p>Connecting RecordPlayer…</p>;
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
  if (!lifecycleRef.current) {
    lifecycleRef.current = new JazzLifecycle(
      accounts,
      (account) => createJazzClient({ appId, env: "dev", serverUrl, account }),
      setClient,
    );
  }
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
        .catch((cause) => console.error("RecordPlayer Jazz shutdown failed", cause));
    },
    [lifecycle],
  );

  const visibleClient = client && lifecycle.isCurrent(principal, sessionId) ? client : undefined;
  if (error && !visibleClient)
    return (
      <section>
        <p role="alert">Could not connect RecordPlayer: {error.message}</p>
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
  if (!visibleClient) return <p>Connecting RecordPlayer…</p>;
  return (
    <LifecycleContext.Provider value={lifecycle}>
      {error && <p role="alert">Could not update RecordPlayer: {error.message}</p>}
      <JazzClientProvider client={visibleClient}>{children}</JazzClientProvider>
    </LifecycleContext.Provider>
  );
}

async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a Jazz session token.");
  return token;
}

function SignIn() {
  const [email, setEmail] = useState("listener@example.com");
  const [password, setPassword] = useState("record-player-demo");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  async function authenticate(mode: "sign-in" | "sign-up") {
    sessionStorage.setItem(registerIntentKey, mode === "sign-up" ? "1" : "0");
    setPending(true);
    setError(null);
    try {
      const result =
        mode === "sign-in"
          ? await authClient.signIn.email({ email, password })
          : await authClient.signUp.email({ email, password, name: email });
      if (result.error) {
        sessionStorage.removeItem(registerIntentKey);
        setError(result.error.message ?? "Authentication failed");
      }
    } catch (cause) {
      sessionStorage.removeItem(registerIntentKey);
      setError(cause instanceof Error ? cause.message : "Authentication failed");
    } finally {
      setPending(false);
    }
  }
  return (
    <section>
      <h2>Sign in to RecordPlayer</h2>
      <label>
        Email
        <input value={email} onChange={(event) => setEmail(event.target.value)} />
      </label>
      <label>
        Password
        <input
          type="password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
        />
      </label>
      <button disabled={pending} onClick={() => void authenticate("sign-in")}>
        Sign in
      </button>
      <button disabled={pending} onClick={() => void authenticate("sign-up")}>
        Create account
      </button>
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
