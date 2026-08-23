"use client";

import * as React from "react";
import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { Operations } from "../src/App";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";

function useBetterAuthJwt(sessionId: string | undefined) {
  const [jwt, setJwt] = React.useState<string | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    let cancelled = false;
    setJwt(null);
    setError(null);
    if (!sessionId) return;
    void getJwtFromBetterAuth().then((token) => {
      if (cancelled) return;
      if (token) setJwt(token);
      else setError("Better Auth did not issue a Jazz token.");
    });
    return () => {
      cancelled = true;
    };
  }, [sessionId]);

  return { jwt, error };
}

function usePersonalLabel(sessionId: string | undefined) {
  const [attempt, retry] = React.useReducer((value) => value + 1, 0);
  const [state, setState] = React.useState<"idle" | "loading" | "ready" | "error">("idle");

  React.useEffect(() => {
    let cancelled = false;
    if (!sessionId) {
      setState("idle");
      return;
    }
    setState("loading");
    void fetch("/api/bootstrap", { method: "POST" })
      .then((response) => {
        if (!response.ok) throw new Error(`bootstrap failed (${response.status})`);
        if (!cancelled) setState("ready");
      })
      .catch(() => {
        if (!cancelled) setState("error");
      });
    return () => {
      cancelled = true;
    };
  }, [attempt, sessionId]);

  return { state, retry };
}

function SignIn() {
  const [email, setEmail] = React.useState("label@example.com");
  const [password, setPassword] = React.useState("big-label-demo");
  const [error, setError] = React.useState<string | null>(null);
  const [pending, setPending] = React.useState(false);

  async function authenticate(mode: "sign-in" | "sign-up") {
    setPending(true);
    setError(null);
    const result =
      mode === "sign-in"
        ? await authClient.signIn.email({ email, password })
        : await authClient.signUp.email({ email, password, name: email });
    setPending(false);
    if (result.error) setError(result.error.message ?? "Authentication failed");
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

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;

export default function Page() {
  const { data, isPending } = authClient.useSession();
  const sessionId = data?.session.id;
  const auth = useBetterAuthJwt(sessionId);
  const bootstrap = usePersonalLabel(sessionId);

  const config = React.useMemo<DbConfig>(
    () => ({ appId, env: "dev", serverUrl, jwtToken: auth.jwt! }),
    [auth.jwt],
  );

  if (isPending) return <main className="auth-shell">Loading your session…</main>;
  if (!sessionId) return <SignIn />;
  if (auth.error) return <main className="auth-shell">{auth.error}</main>;
  if (bootstrap.state === "error") {
    return (
      <main className="auth-shell">
        <p>BigLabel could not provision your personal label.</p>
        <button onClick={bootstrap.retry}>Retry</button>
      </main>
    );
  }
  if (!auth.jwt || bootstrap.state !== "ready") {
    return <main className="auth-shell">Preparing your personal label…</main>;
  }

  return (
    <JazzProvider
      config={config}
      onJWTExpired={getJwtFromBetterAuth}
      fallback={<main className="auth-shell">Connecting to BigLabel…</main>}
    >
      <Operations onSignOut={() => void authClient.signOut()} />
    </JazzProvider>
  );
}
