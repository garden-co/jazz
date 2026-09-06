"use client";

import { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";
import { createAccountManager } from "jazz-tools";
import { createJazzClient, JazzClientProvider } from "jazz-tools/react";
import { accounts as prepareAccounts, getToken } from "@/lib/accounts";
import { authClient } from "@/lib/auth-client";
import { JazzLifecycle } from "@/lib/jazz-lifecycle";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
type AuthResult = { error?: { message?: string | null } | null };
interface JazzLifecycleApi {
  transition: JazzLifecycle["transition"];
  authenticate(enroll: boolean, request: () => Promise<AuthResult>): Promise<void>;
  reportFailure(cause: unknown): void;
}
const LifecycleContext = createContext<JazzLifecycleApi | null>(null);

export function useJazzLifecycle() {
  const lifecycle = useContext(LifecycleContext);
  if (!lifecycle) throw new Error("Jazz lifecycle is not ready");
  return lifecycle;
}

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const [manager, setManager] = useState<Awaited<ReturnType<typeof createAccountManager>>>();
  const [error, setError] = useState<Error>();
  useEffect(() => {
    let cancelled = false;
    void prepareAccounts()
      .then((next) => {
        if (!cancelled) setManager(next);
      })
      .catch((cause) => {
        if (!cancelled) setError(toError(cause));
      });
    return () => {
      cancelled = true;
    };
  }, []);
  if (error) return <p role="alert">{error.message}</p>;
  if (!manager || !APP_ID || !SERVER_URL) return <p>Loading...</p>;
  return (
    <AccountContext manager={manager} appId={APP_ID} serverUrl={SERVER_URL}>
      {children}
    </AccountContext>
  );
}

function AccountContext({
  manager,
  appId,
  serverUrl,
  children,
}: {
  manager: Awaited<ReturnType<typeof createAccountManager>>;
  appId: string;
  serverUrl: string;
  children: React.ReactNode;
}) {
  const { data: session, isPending } = authClient.useSession();
  const [client, setClient] = useState<Awaited<ReturnType<typeof createJazzClient>>>();
  const [setupError, setSetupError] = useState<Error>();
  const [recovery, setRecovery] = useState<"login" | "register">("register");
  const [admittedSession, setAdmittedSession] = useState<string | null>(null);
  const [reconcileGeneration, rerunSessionSelection] = useState(0);
  const lifecycleRef = useRef<JazzLifecycle | undefined>(undefined);
  const booted = useRef(false);
  const explicitAuth = useRef(false);
  const sessionVersion = useRef(0);
  const sessionKeyRef = useRef<string | null>(null);
  const handledSession = useRef<string | null | undefined>(undefined);
  if (!lifecycleRef.current) {
    lifecycleRef.current = new JazzLifecycle(
      manager,
      (account) => createJazzClient({ appId, serverUrl, account }),
      setClient,
    );
  }
  const lifecycle = lifecycleRef.current;
  const key = sessionKey(session);
  sessionKeyRef.current = key;

  useEffect(() => {
    if (isPending) return;
    if (!booted.current) {
      booted.current = true;
      handledSession.current = key;
      const version = ++sessionVersion.current;
      void lifecycle
        .attach(async () => {
          if (key) await manager.loginJWT({ getToken });
        })
        .then(() => {
          if (version === sessionVersion.current) setAdmittedSession(key);
        })
        .catch((cause) => {
          if (version === sessionVersion.current) {
            setRecovery("login");
            setSetupError(toError(cause));
          }
        });
      return;
    }
    if (explicitAuth.current) return;
    if (key === handledSession.current) {
      void lifecycle.attach(async () => {}).catch((cause) => setSetupError(toError(cause)));
      return;
    }
    handledSession.current = key;
    setAdmittedSession(null);
    const version = ++sessionVersion.current;
    void lifecycle
      .transition(
        async (accounts) => {
          if (key) await accounts.loginJWT({ getToken });
          else accounts.logout();
        },
        () => !explicitAuth.current && version === sessionVersion.current,
        false,
      )
      .then(() => {
        if (version === sessionVersion.current) {
          setSetupError(undefined);
          setAdmittedSession(key);
        }
      })
      .catch((cause) => {
        if (version === sessionVersion.current) {
          setRecovery("login");
          setSetupError(toError(cause));
        }
      });
  }, [isPending, key, lifecycle, manager, reconcileGeneration]);

  useEffect(
    () => () => {
      void lifecycle.close().catch((cause) => console.error("Jazz client shutdown failed", cause));
    },
    [lifecycle],
  );

  const authenticate = useCallback(
    async (enroll: boolean, request: () => Promise<AuthResult>) => {
      explicitAuth.current = true;
      const version = ++sessionVersion.current;
      try {
        const result = await request();
        if (result.error)
          throw new Error(result.error.message ?? (enroll ? "Sign-up failed" : "Sign-in failed"));
        const current = await authClient.getSession();
        handledSession.current = sessionKey(current.data);
        await lifecycle.transition(
          (accounts) =>
            enroll ? accounts.registerJWT({ getToken }) : accounts.loginJWT({ getToken }),
          () => explicitAuth.current && version === sessionVersion.current,
        );
        setSetupError(undefined);
        setAdmittedSession(handledSession.current ?? null);
      } catch (cause) {
        const error = toError(cause);
        if (sessionKey((await authClient.getSession()).data)) {
          setRecovery(enroll ? "register" : "login");
          setSetupError(error);
        }
        throw error;
      } finally {
        explicitAuth.current = false;
        rerunSessionSelection((value) => value + 1);
      }
    },
    [lifecycle],
  );

  const api: JazzLifecycleApi = {
    transition: lifecycle.transition.bind(lifecycle),
    authenticate,
    reportFailure(cause) {
      setSetupError(toError(cause));
    },
  };
  const recover = () => {
    const version = ++sessionVersion.current;
    const recoveryKey = key;
    void lifecycle
      .transition(
        (accounts) =>
          recovery === "login"
            ? accounts.loginJWT({ getToken })
            : accounts.registerJWT({ getToken }),
        () => version === sessionVersion.current && sessionKeyRef.current === recoveryKey,
        recovery !== "login",
      )
      .then(() => {
        if (version === sessionVersion.current && sessionKeyRef.current === recoveryKey) {
          setSetupError(undefined);
          setAdmittedSession(recoveryKey);
        }
      })
      .catch((cause) => {
        if (version === sessionVersion.current && sessionKeyRef.current === recoveryKey)
          setSetupError(toError(cause));
      });
  };
  return (
    <LifecycleContext.Provider value={api}>
      {client && admittedSession === key ? (
        <JazzClientProvider client={client}>
          {setupError && (
            <aside className="alert-error" role="alert">
              {setupError.message}
            </aside>
          )}
          {children}
        </JazzClientProvider>
      ) : setupError ? (
        <main className="page-center">
          <div className="card">
            <p className="alert-error" role="alert">
              {setupError.message}
            </p>
            <button type="button" className="btn-primary" onClick={recover}>
              {recovery === "login" ? "Retry sign in" : "Complete account setup"}
            </button>
          </div>
        </main>
      ) : session ? (
        <p>Loading...</p>
      ) : (
        children
      )}
    </LifecycleContext.Provider>
  );
}

function sessionKey(
  session: { session?: { id?: string } | null; user?: { id?: string } | null } | null | undefined,
) {
  return session?.session?.id ?? session?.user?.id ?? null;
}
function toError(cause: unknown) {
  return cause instanceof Error ? cause : new Error(String(cause));
}
