"use client";

import { useEffect, useMemo, useState } from "react";
import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { authClient, getJwtFromBetterAuth } from "../src/lib/auth-client";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!;

export function RecordPlayerProvider({ children }: { children: React.ReactNode }) {
  const { data: session, isPending } = authClient.useSession();
  const principal = session?.user.id;
  const [jwt, setJwt] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setJwt(null);
    if (!principal) return;
    void getJwtFromBetterAuth().then((token) => {
      if (!cancelled) setJwt(token ?? null);
    });
    return () => {
      cancelled = true;
    };
  }, [principal]);

  const config = useMemo<DbConfig | null>(
    () => (jwt ? { appId, env: "dev", serverUrl, jwtToken: jwt } : null),
    [jwt],
  );

  if (isPending) return <p>Preparing your RecordPlayer…</p>;
  if (!principal) return <SignIn />;
  if (!config) return <p>Connecting RecordPlayer…</p>;

  return (
    <JazzProvider
      config={config}
      onJWTExpired={getJwtFromBetterAuth}
      fallback={<p>Connecting RecordPlayer…</p>}
    >
      {children}
    </JazzProvider>
  );
}

function SignIn() {
  const [email, setEmail] = useState("listener@example.com");
  const [password, setPassword] = useState("record-player-demo");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

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
