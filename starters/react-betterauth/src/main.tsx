import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import {
  useJazzSessionOwner,
  JazzSessionProvider,
  type JazzSession,
  type JazzClient,
} from "jazz-tools/react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./App.css";
import { getToken } from "./accounts";
import { authClient } from "./auth-client";
const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;
function SessionOwner({ children }: React.PropsWithChildren) {
  const {
    session: jazz,
    error,
    retry,
  } = useJazzSessionOwner({ appId: APP_ID!, serverUrl: SERVER_URL! });
  if (error)
    return (
      <section>
        <p role="alert">{error.message}</p>
        <button onClick={() => void retry().catch(() => {})}>Retry</button>
      </section>
    );
  if (!jazz) return <p>Loading...</p>;
  return <AccountContext jazz={jazz}>{children}</AccountContext>;
}
type AuthResult = { error?: { message?: string | null } | null };
interface AuthActions {
  authenticate(enroll: boolean, request: () => Promise<AuthResult>): Promise<void>;
  signOut(): Promise<void>;
  reportFailure(cause: unknown): void;
}
const AuthActionsContext = createContext<AuthActions | null>(null);
export function useAuthActions() {
  const actions = useContext(AuthActionsContext);
  if (!actions) throw new Error("Authentication is not ready");
  return actions;
}

function AccountContext({
  jazz,
  children,
}: React.PropsWithChildren<{ jazz: JazzSession<JazzClient> }>) {
  const { data: auth, isPending } = authClient.useSession();
  const snapshot = useSyncExternalStore(jazz.subscribe, jazz.getSnapshot, jazz.getSnapshot);
  const key = auth?.session.id ?? null;
  const currentKey = useRef(key);
  currentKey.current = key;
  const attempted = useRef<string | null | undefined>(undefined);
  const working = useRef(false);
  const [revision, reconcile] = useState(0);
  const [admitted, setAdmitted] = useState<string | null>(null);
  const [error, setError] = useState<Error>();
  const [recovery, setRecovery] = useState<"login" | "register" | "signout">("login");

  useEffect(() => {
    if (recovery === "signout" && error) return;
    if (isPending || working.current || attempted.current === key) return;
    attempted.current = key;
    working.current = true;
    setAdmitted(null);
    void (key ? jazz.loginJWT({ getToken }) : jazz.logout())
      .then(
        () => {
          if (currentKey.current === key) {
            setAdmitted(key);
            setError(undefined);
          }
        },
        (cause) => {
          if (currentKey.current === key) {
            setRecovery("login");
            setError(toError(cause));
          }
        },
      )
      .finally(() => {
        working.current = false;
        reconcile((value) => value + 1);
      });
  }, [isPending, key, jazz, revision, recovery, error]);

  const actions: AuthActions = {
    async authenticate(enroll, request) {
      if (working.current) throw new Error("An authentication request is already pending");
      working.current = true;
      setAdmitted(null);
      let authenticatedKey: string | null = null;
      try {
        const result = await request();
        if (result.error) throw new Error(result.error.message ?? "Authentication failed");
        const current = await authClient.getSession();
        authenticatedKey = current.data?.session.id ?? null;
        if (!authenticatedKey) throw new Error("Better Auth did not establish a session");
        attempted.current = authenticatedKey;
        await (enroll ? jazz.registerJWT({ getToken }) : jazz.loginJWT({ getToken }));
        setAdmitted(authenticatedKey);
        setError(undefined);
      } catch (cause) {
        if (authenticatedKey) {
          setRecovery(enroll ? "register" : "login");
          setError(toError(cause));
        }
        throw cause;
      } finally {
        working.current = false;
        reconcile((value) => value + 1);
      }
    },
    async signOut() {
      if (working.current) throw new Error("An authentication request is already pending");
      working.current = true;
      setRecovery("signout");
      try {
        // Jazz syncs and detaches data consumers before the auth provider revokes credentials.
        await jazz.logout();
        const result = await authClient.signOut();
        if (result.error) throw new Error(result.error.message ?? "Sign out failed");
        window.location.assign("/");
      } catch (cause) {
        setError(toError(cause));
        throw cause;
      } finally {
        working.current = false;
        reconcile((value) => value + 1);
      }
    },
    reportFailure(cause) {
      setError(toError(cause));
    },
  };
  async function recover() {
    if (working.current) return;
    if (recovery === "signout") {
      await actions.signOut().catch(() => {});
      return;
    }
    working.current = true;
    const recoveringKey = key;
    try {
      // A successful registry action can leave a selected account whose client
      // failed to open. Retry startup instead of repeating registration.
      if (snapshot.status === "error" && snapshot.account?.identity.subject === auth?.user.id) {
        await jazz.retry();
      } else if (recovery === "register") {
        await jazz.registerJWT({ getToken });
      } else {
        await jazz.loginJWT({ getToken });
      }
      if (currentKey.current === recoveringKey) {
        setAdmitted(recoveringKey);
        setError(undefined);
      }
    } catch (cause) {
      if (currentKey.current === recoveringKey) setError(toError(cause));
    } finally {
      working.current = false;
      reconcile((value) => value + 1);
    }
  }
  const failure = error ?? snapshot.error;
  const ready =
    snapshot.status === "ready" &&
    admitted === key &&
    snapshot.account?.identity.subject === auth?.user.id;
  const fallback = failure ? (
    <main className="page-center">
      <div className="card">
        <p className="alert-error" role="alert">
          {failure.message}
        </p>
        <button
          type="button"
          className="btn-primary"
          onClick={() => void recover()}
          disabled={snapshot.status === "transitioning"}
        >
          {recovery === "signout"
            ? "Retry sign out"
            : recovery === "login"
              ? "Retry sign in"
              : "Complete account setup"}
        </button>
      </div>
    </main>
  ) : isPending || auth ? (
    <p>Loading...</p>
  ) : (
    children
  );
  return (
    <AuthActionsContext.Provider value={actions}>
      <JazzSessionProvider session={jazz} fallback={fallback}>
        {ready ? (
          <>
            {failure && (
              <aside className="alert-error" role="alert">
                {failure.message}
              </aside>
            )}
            {children}
          </>
        ) : (
          fallback
        )}
      </JazzSessionProvider>
    </AuthActionsContext.Provider>
  );
}
function toError(cause: unknown) {
  return cause instanceof Error ? cause : new Error(String(cause));
}

createRoot(document.getElementById("root")!).render(
  <SessionOwner>
    <App />
  </SessionOwner>,
);
