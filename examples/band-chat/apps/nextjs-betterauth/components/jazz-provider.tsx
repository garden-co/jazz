"use client";
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
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";
const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
const registerIntentKey = "band-chat-register-jwt";
type AuthActions = { signOut(): Promise<void> };
const AuthContext = createContext<AuthActions | null>(null);
export function useBandChatLifecycle(): AuthActions {
  const actions = useContext(AuthContext);
  if (!actions) throw new Error("BandChat authentication is not ready");
  return actions;
}
export function JazzProvider({ children }: React.PropsWithChildren) {
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

function AccountContext({
  jazz,
  children,
}: React.PropsWithChildren<{ jazz: JazzSession<JazzClient> }>) {
  const { data: auth, isPending } = authClient.useSession();
  const snapshot = useSyncExternalStore(jazz.subscribe, jazz.getSnapshot, jazz.getSnapshot);
  const key = auth?.session.id ?? null;
  const principal = auth?.user.id;
  const currentKey = useRef(key);
  currentKey.current = key;
  const attempted = useRef<string | null | undefined>(undefined);
  const working = useRef(false);
  const [revision, reconcile] = useState(0);
  const [admitted, setAdmitted] = useState<string | null>(null);
  const [error, setError] = useState<Error>();
  const [failedAction, setFailedAction] = useState<"connect" | "signout">("connect");

  async function connect(retry = false) {
    if (working.current) return;
    setFailedAction("connect");
    attempted.current = key;
    working.current = true;
    setAdmitted(null);
    try {
      if (!key) await jazz.logout();
      else if (
        retry &&
        snapshot.status === "error" &&
        snapshot.account?.identity.subject === principal
      ) {
        await jazz.retry();
      } else if (sessionStorage.getItem(registerIntentKey) === "1") {
        await jazz.registerJWT({ getToken: requireBetterAuthToken });
      } else {
        // Every Better Auth session id is a fresh authentication boundary, even
        // when its subject matches the retained Jazz account.
        await jazz.loginJWT({ getToken: requireBetterAuthToken });
      }
      if (currentKey.current !== key) return;
      if (key && jazz.getSnapshot().account?.identity.subject !== principal) {
        throw new Error("Jazz selected an account for a different signed-in user.");
      }
      setAdmitted(key);
      setError(undefined);
      if (key) sessionStorage.removeItem(registerIntentKey);
    } catch (cause) {
      if (currentKey.current === key) setError(toError(cause));
    } finally {
      working.current = false;
      reconcile((value) => value + 1);
    }
  }
  useEffect(() => {
    if (failedAction === "signout" && error) return;
    if (!isPending && !working.current && attempted.current !== key) void connect();
  }, [isPending, key, revision, failedAction, error]);

  const actions: AuthActions = {
    async signOut() {
      if (working.current) return;
      working.current = true;
      setFailedAction("signout");
      try {
        await jazz.logout();
        const result = await authClient.signOut();
        if (result.error) throw new Error(result.error.message ?? "Sign out failed");
        window.location.assign("/");
      } catch (cause) {
        setError(toError(cause));
      } finally {
        working.current = false;
        reconcile((value) => value + 1);
      }
    },
  };
  const failure = error ?? snapshot.error;
  const fallback = failure ? (
    <section>
      <p role="alert">
        {failedAction === "signout"
          ? "Could not sign out of BandChat"
          : "Could not connect BandChat"}
        : {failure.message}
      </p>
      <button
        onClick={() => void (failedAction === "signout" ? actions.signOut() : connect(true))}
        disabled={snapshot.status === "transitioning"}
      >
        {failedAction === "signout" ? "Retry sign out" : "Retry connection"}
      </button>
    </section>
  ) : (
    <p className="loading-state">Connecting BandChat…</p>
  );
  const ready =
    !isPending && key && admitted === key && snapshot.account?.identity.subject === principal;
  return (
    <AuthContext.Provider value={actions}>
      <JazzSessionProvider session={jazz} fallback={fallback}>
        {ready ? (
          <>
            {failure && <p role="alert">Could not update BandChat: {failure.message}</p>}
            {children}
          </>
        ) : (
          fallback
        )}
      </JazzSessionProvider>
    </AuthContext.Provider>
  );
}
async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not provide a Jazz session token.");
  return token;
}
function toError(cause: unknown) {
  return cause instanceof Error ? cause : new Error(String(cause));
}
