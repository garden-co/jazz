"use client";
import { useEffect, useState, type FormEvent } from "react";
import { App } from "../src/App";
import { authClient } from "../src/lib/auth-client";

export default function Page() {
  const { data: session, isPending } = authClient.useSession();
  const [jwtToken, setJwtToken] = useState<string | null>(null);
  const [bootstrap, setBootstrap] = useState<"idle" | "working" | "ready" | "error">("idle");
  useEffect(() => {
    if (!session?.session.id) {
      setJwtToken(null);
      setBootstrap("idle");
      return;
    }
    let cancelled = false;
    setBootstrap("working");
    void fetch("/api/bootstrap", { method: "POST" })
      .then(async (response) => {
        if (!response.ok) throw new Error("profile bootstrap failed");
        const token = await authClient.token();
        if (!cancelled && token.data?.token) {
          setJwtToken(token.data.token);
          setBootstrap("ready");
        }
      })
      .catch(() => {
        if (!cancelled) setBootstrap("error");
      });
    return () => {
      cancelled = true;
    };
  }, [session?.session.id]);
  if (isPending) return <p className="shell">Validating session…</p>;
  if (!session?.user) return <SignIn />;
  if (bootstrap === "working" || bootstrap === "idle")
    return <p className="shell">Provisioning your BandChat profile…</p>;
  if (bootstrap === "error" || !jwtToken)
    return <p className="shell">Could not provision your profile. Refresh to retry.</p>;
  return (
    <App
      config={{
        appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
        jwtToken,
      }}
    />
  );
}

function SignIn() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setError(null);
    const signedIn = await authClient.signIn.email({ email, password });
    if (!signedIn.error) return;
    const signedUp = await authClient.signUp.email({
      email,
      password,
      name: email.split("@")[0] || "Musician",
    });
    if (signedUp.error) setError(signedUp.error.message ?? "Could not sign in");
  };
  return (
    <main className="shell">
      <header>
        <span className="eyebrow">LOCAL-FIRST BAND HQ</span>
        <h1>BandChat</h1>
        <p>
          Sign in with Better Auth. The server provisions your Jazz profile before ordinary data
          reads mount.
        </p>
      </header>
      <section className="empty">
        <form onSubmit={(event) => void submit(event)}>
          <input
            aria-label="Email"
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            required
          />
          <input
            aria-label="Password"
            type="password"
            minLength={8}
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            required
          />
          <button type="submit">Sign in or create account</button>
        </form>
        {error && <p role="alert">{error}</p>}
      </section>
    </main>
  );
}
