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
import { JazzLifecycle } from "@/lib/jazz-lifecycle";

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
    return <p className="loading-state">Opening your Jazz account…</p>;
  if (slot.error)
    return (
      <p className="loading-state" role="alert">
        {slot.error.message} <button onClick={() => setRetry((value) => value + 1)}>Retry</button>
      </p>
    );
  if (!slot.manager) return <p className="loading-state">Opening your Jazz account…</p>;
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
      (account) => createJazzClient({ appId: APP_ID, serverUrl: SERVER_URL, account }),
      setClient,
    );
  const lifecycle = lifecycleRef.current;

  const enrollAndBootstrap = useCallback(async () => {
    const registering = claimsSignupIntent(email, identityId);
    setError(undefined);
    setReady(false);
    await lifecycle.transition(
      (manager) =>
        registering
          ? manager.registerJWT({ getToken: getJazzToken })
          : manager.loginJWT({ getToken: getJazzToken }),
      () => active.current,
    );
    if (registering) clearSignupIntent();
    const token = await getJazzToken();
    const response = await fetch("/api/bootstrap", {
      method: "POST",
      headers: { authorization: `Bearer ${token}` },
    });
    if (!response.ok) throw new Error(`Profile bootstrap failed (${response.status})`);
    if (active.current) setReady(true);
  }, [email, identityId, lifecycle]);

  useEffect(() => {
    active.current = true;
    void enrollAndBootstrap().catch((cause) => {
      if (active.current) setError(toError(cause));
    });
    return () => {
      active.current = false;
      // A late open sees active=false and retires itself instead of publishing
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
    return (
      <p className="loading-state" role="alert">
        {error.message} <button onClick={() => setRetry((value) => value + 1)}>Retry</button>
      </p>
    );
  if (!client || !ready) return <p className="loading-state">Opening your Jazz account…</p>;
  return (
    <JazzClientProvider client={client}>
      <SignOutContext.Provider value={signOut}>{children}</SignOutContext.Provider>
    </JazzClientProvider>
  );
}
