"use client";
import { createContext, useContext } from "react";
import {
  useJazzSessionOwner,
  JazzSessionProvider,
  useBetterAuth,
  type JazzSession,
  type JazzClient,
} from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";
const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
function SessionOwner({ children }: React.PropsWithChildren) {
  const { session, error, retry } = useJazzSessionOwner({ appId: APP_ID!, serverUrl: SERVER_URL! });
  if (error)
    return (
      <section>
        <p role="alert">{error.message}</p>
        <button onClick={() => void retry().catch(() => {})}>Retry</button>
      </section>
    );
  if (!session) return <p>Loading...</p>;
  return <AccountContext jazz={session}>{children}</AccountContext>;
}
const AuthActionsContext = createContext<{ signOut(): Promise<void> } | null>(null);
export function useAuthActions() {
  const actions = useContext(AuthActionsContext);
  if (!actions) throw new Error("Authentication is not ready");
  return actions;
}
function AccountContext({
  jazz,
  children,
}: React.PropsWithChildren<{ jazz: JazzSession<JazzClient> }>) {
  const auth = useBetterAuth(jazz, authClient);
  const fallback = auth.error ? (
    <section>
      <p role="alert">{auth.error.message}</p>
      <button onClick={() => void auth.retry().catch(() => {})}>Retry</button>
    </section>
  ) : auth.isPending ? (
    <p>Loading...</p>
  ) : (
    children
  );
  return (
    <AuthActionsContext.Provider value={{ signOut: auth.logout }}>
      <JazzSessionProvider session={jazz} fallback={fallback}>
        {auth.ready ? children : fallback}
      </JazzSessionProvider>
    </AuthActionsContext.Provider>
  );
}

export { SessionOwner as JazzProvider };
