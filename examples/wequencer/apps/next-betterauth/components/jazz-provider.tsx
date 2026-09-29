"use client";

import { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";
import {
  createAccountManager,
  createJazzClient,
  JazzClientProvider,
  type AccountManager,
  type JazzClient,
  type JWTAuth,
} from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";
import { JAZZ_ENV } from "@/lib/jazz-env";
import { JazzLifecycle } from "@/lib/jazz-lifecycle";
import { ErrorScreen, LoadingScreen } from "@/components/status-screen";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
export const SIGNUP_MARKER = "wequencer-register-external-account";
const SignOutContext = createContext<(() => Promise<void>) | null>(null);

type ManagerSlot = {
  sessionId: string;
  identityId: string;
  manager?: AccountManager<JWTAuth>;
  error?: Error;
};
type SignupIntent = { email: string; identityId?: string };

export function beginSignupIntent(email: string) {
  sessionStorage.setItem(SIGNUP_MARKER, JSON.stringify({ email } satisfies SignupIntent));
}

export function clearSignupIntent() {
  sessionStorage.removeItem(SIGNUP_MARKER);
}

function claimsSignupIntent(email: string, identityId: string) {
  const encoded = sessionStorage.getItem(SIGNUP_MARKER);
  if (!encoded) return false;
  try {
    const intent = JSON.parse(encoded) as SignupIntent;
    if (intent.email !== email) return false;
    if (intent.identityId && intent.identityId !== identityId) return false;
    if (!intent.identityId)
      sessionStorage.setItem(SIGNUP_MARKER, JSON.stringify({ ...intent, identityId }));
    return true;
  } catch {
    clearSignupIntent();
    return false;
  }
}

async function getJazzToken() {
  const { data, error } = await authClient.$fetch<{ token: string }>("/token", { method: "GET" });
  if (error || !data?.token)
    throw new Error(error?.message ?? "Better Auth did not issue a Jazz token.");
  return data.token;
}

function toError(cause: unknown) {
  return cause instanceof Error ? cause : new Error(String(cause));
}

export function useGracefulSignOut() {
  const signOut = useContext(SignOutContext);
  if (!signOut) throw new Error("useGracefulSignOut requires JazzProvider");
  return signOut;
}

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = authClient.useSession();
  const [slot, setSlot] = useState<ManagerSlot | null>(null);
  const [retry, setRetry] = useState(0);
  const sessionId = session?.session.id;
  const identityId = session?.user.id;

  useEffect(() => {
    if (!sessionId || !identityId) {
      setSlot(null);
      return;
    }
    let cancelled = false;
    setSlot({ sessionId, identityId });
    void createAccountManager({ appId: APP_ID, serverUrl: SERVER_URL, env: "wequencer" })
      .then((manager) => {
        if (!cancelled) setSlot({ sessionId, identityId, manager });
      })
      .catch((cause) => {
        if (!cancelled) setSlot({ sessionId, identityId, error: toError(cause) });
      });
    return () => {
      cancelled = true;
    };
  }, [identityId, retry, sessionId]);

  if (!sessionId || !identityId) return <>{children}</>;
  // Pair the manager with the external session before rendering any account
  // client. A new session can therefore never observe the prior session's Db.
  if (slot?.sessionId !== sessionId || slot.identityId !== identityId)
    return <LoadingScreen label="Opening your Jazz account…" />;
  if (slot.error)
    return (
      <ErrorScreen message={slot.error.message} onRetry={() => setRetry((value) => value + 1)} />
    );
  if (!slot.manager) return <LoadingScreen label="Opening your Jazz account…" />;
  return (
    <EnrolledProvider
      key={sessionId}
      accounts={slot.manager}
      email={session.user.email}
      identityId={identityId}
    >
      {children}
    </EnrolledProvider>
  );
}

function EnrolledProvider({
  accounts,
  email,
  identityId,
  children,
}: {
  accounts: AccountManager<JWTAuth>;
  email: string;
  identityId: string;
  children: React.ReactNode;
}) {
  const [client, setClient] = useState<JazzClient>();
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<Error>();
  const [retry, setRetry] = useState(0);
  const active = useRef(true);
  const lifecycleRef = useRef<JazzLifecycle | undefined>(undefined);
  if (!lifecycleRef.current)
    lifecycleRef.current = new JazzLifecycle(
      accounts,
      (account) =>
        createJazzClient({ appId: APP_ID, serverUrl: SERVER_URL, env: JAZZ_ENV, account }),
      setClient,
    );
  const lifecycle = lifecycleRef.current;

  const enrollAndBootstrap = useCallback(
    async (isCurrent: () => boolean) => {
      const registering = claimsSignupIntent(email, identityId);
      setError(undefined);
      setReady(false);
      await lifecycle.transition(
        (manager) =>
          registering
            ? manager.registerJWT({ getToken: getJazzToken })
            : manager.loginJWT({ getToken: getJazzToken }),
        isCurrent,
      );
      if (!isCurrent()) return;
      if (registering) clearSignupIntent();
      const token = await getJazzToken();
      const response = await fetch("/api/bootstrap", {
        method: "POST",
        headers: { authorization: `Bearer ${token}` },
      });
      if (!response.ok) throw new Error(`Profile bootstrap failed (${response.status})`);
      if (isCurrent()) setReady(true);
    },
    [email, identityId, lifecycle],
  );

  useEffect(() => {
    // Each run gets its own token. React may start this effect, clean it up and
    // start it again before the first run's queued transition executes; the
    // abandoned run must then skip registration rather than register twice.
    let current = true;
    const isCurrent = () => current;
    active.current = true;
    void enrollAndBootstrap(isCurrent).catch((cause) => {
      if (isCurrent()) setError(toError(cause));
    });
    return () => {
      current = false;
      active.current = false;
      // A late open sees a stale token and retires itself instead of publishing
      // into a replacement session. The cleanup rejection is observed here.
      void lifecycle.close().catch((cause) => console.error("Jazz shutdown failed", cause));
    };
  }, [enrollAndBootstrap, lifecycle, retry]);

  const signOut = useCallback(async () => {
    try {
      await lifecycle.transition(async (manager) => {
        // Preserve the selection until the external provider succeeds. A
        // provider failure is followed by reopening this selected account.
        await authClient.signOut();
        manager.logout();
      });
    } catch (cause) {
      const next = toError(cause);
      if (active.current) setError(next);
      throw next;
    }
  }, [lifecycle]);

  if (error)
    return <ErrorScreen message={error.message} onRetry={() => setRetry((value) => value + 1)} />;
  if (!client || !ready) return <LoadingScreen label="Opening your Jazz account…" />;
  return (
    <JazzClientProvider client={client}>
      <SignOutContext.Provider value={signOut}>{children}</SignOutContext.Provider>
    </JazzClientProvider>
  );
}
