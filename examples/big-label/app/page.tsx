"use client";

import * as React from "react";
import {
  createAccountManager,
  createJazzClient,
  JazzClientProvider,
  type AccountManager,
  type JazzClient,
  type JWTAuth,
} from "jazz-tools/react";
import { Operations } from "../src/App";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";
import { JazzLifecycle } from "../src/lib/jazz-lifecycle";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;
const signupMarker = "big-label-register-external-account";

type SignupIntent = { email: string; identityId?: string };
type ManagerSlot = {
  sessionId: string;
  identityId: string;
  manager?: AccountManager<JWTAuth>;
  error?: Error;
};

function beginSignupIntent(email: string) {
  sessionStorage.setItem(signupMarker, JSON.stringify({ email } satisfies SignupIntent));
}

function clearSignupIntent() {
  sessionStorage.removeItem(signupMarker);
}

function claimsSignupIntent(email: string, identityId: string) {
  const encoded = sessionStorage.getItem(signupMarker);
  if (!encoded) return false;
  try {
    const intent = JSON.parse(encoded) as SignupIntent;
    if (intent.email !== email) return false;
    if (intent.identityId && intent.identityId !== identityId) return false;
    if (!intent.identityId)
      sessionStorage.setItem(signupMarker, JSON.stringify({ ...intent, identityId }));
    return true;
  } catch {
    clearSignupIntent();
    return false;
  }
}

function toError(cause: unknown) {
  return cause instanceof Error ? cause : new Error(String(cause));
}

function SignIn() {
  const [email, setEmail] = React.useState("label@example.com");
  const [password, setPassword] = React.useState("big-label-demo");
  const [error, setError] = React.useState<string | null>(null);
  const [pending, setPending] = React.useState(false);
  async function authenticate(mode: "sign-in" | "sign-up") {
    setPending(true);
    setError(null);
    if (mode === "sign-up") beginSignupIntent(email);
    const result =
      mode === "sign-in"
        ? await authClient.signIn.email({ email, password })
        : await authClient.signUp.email({ email, password, name: email });
    setPending(false);
    if (result.error) {
      if (mode === "sign-up") clearSignupIntent();
      setError(result.error.message ?? "Authentication failed");
    }
  }
  return (
    <main className="auth-shell">
      <h1>BigLabel</h1>
      <p>Sign in to provision and operate your label.</p>
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
      <div>
        <button disabled={pending} onClick={() => void authenticate("sign-in")}>
          Sign in
        </button>
        <button disabled={pending} onClick={() => void authenticate("sign-up")}>
          Create account
        </button>
      </div>
      {error && <p role="alert">{error}</p>}
    </main>
  );
}

export default function Page() {
  const { data, isPending } = authClient.useSession();
  const [slot, setSlot] = React.useState<ManagerSlot | null>(null);
  const [retry, setRetry] = React.useState(0);
  const sessionId = data?.session.id;
  const identityId = data?.user.id;

  React.useEffect(() => {
    if (!sessionId || !identityId) {
      setSlot(null);
      return;
    }
    let cancelled = false;
    setSlot({ sessionId, identityId });
    void createAccountManager({ appId, serverUrl, env: "big-label" })
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

  if (isPending) return <main className="auth-shell">Loading your session…</main>;
  if (!sessionId || !identityId || !data) return <SignIn />;
  // Never let session B render session A's manager or client while effects
  // prepare the replacement manager.
  if (slot?.sessionId !== sessionId || slot.identityId !== identityId)
    return <main className="auth-shell">Preparing your personal label…</main>;
  if (slot.error)
    return (
      <main className="auth-shell" role="alert">
        {slot.error.message} <button onClick={() => setRetry((value) => value + 1)}>Retry</button>
      </main>
    );
  if (!slot.manager) return <main className="auth-shell">Preparing your personal label…</main>;
  return (
    <AccountApp
      key={sessionId}
      accounts={slot.manager}
      email={data.user.email}
      identityId={identityId}
    />
  );
}

function AccountApp({
  accounts,
  email,
  identityId,
}: {
  accounts: AccountManager<JWTAuth>;
  email: string;
  identityId: string;
}) {
  const [client, setClient] = React.useState<JazzClient>();
  const [ready, setReady] = React.useState(false);
  const [error, setError] = React.useState<Error>();
  const [retry, setRetry] = React.useState(0);
  const active = React.useRef(true);
  const lifecycleRef = React.useRef<JazzLifecycle | undefined>(undefined);
  if (!lifecycleRef.current)
    lifecycleRef.current = new JazzLifecycle(
      accounts,
      (account) => createJazzClient({ appId, serverUrl, account }),
      setClient,
    );
  const lifecycle = lifecycleRef.current;

  const enrollAndBootstrap = React.useCallback(async () => {
    const registering = claimsSignupIntent(email, identityId);
    setError(undefined);
    setReady(false);
    await lifecycle.transition(
      (manager) =>
        registering
          ? manager.registerJWT({ getToken: requireJazzToken })
          : manager.loginJWT({ getToken: requireJazzToken }),
      () => active.current,
    );
    if (registering) clearSignupIntent();
    const token = await requireJazzToken();
    const response = await fetch("/api/bootstrap", {
      method: "POST",
      headers: { authorization: `Bearer ${token}` },
    });
    if (!response.ok) throw new Error(`bootstrap failed (${response.status})`);
    if (active.current) setReady(true);
  }, [email, identityId, lifecycle]);

  React.useEffect(() => {
    active.current = true;
    void enrollAndBootstrap().catch((cause) => {
      if (active.current) setError(toError(cause));
    });
    return () => {
      active.current = false;
      void lifecycle.close().catch((cause) => console.error("Jazz shutdown failed", cause));
    };
  }, [enrollAndBootstrap, lifecycle, retry]);

  const signOut = React.useCallback(async () => {
    try {
      await lifecycle.transition(async (manager) => {
        // Keep the account selected until Better Auth succeeds. If it rejects,
        // the lifecycle reopens the selected client and this error stays here.
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
      <main className="auth-shell" role="alert">
        {error.message} <button onClick={() => setRetry((value) => value + 1)}>Retry</button>
      </main>
    );
  if (!client || !ready) return <main className="auth-shell">Preparing your personal label…</main>;
  return (
    <JazzClientProvider client={client}>
      <Operations onSignOut={() => void signOut().catch(() => {})} />
    </JazzClientProvider>
  );
}

async function requireJazzToken() {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Better Auth did not issue a Jazz token.");
  return token;
}
