import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { useEffect, useRef, useState, type FormEvent } from "react";
import { Timeline } from "./Timeline.js";
import { LoadingScreen } from "./TimelineView.js";

const appId = import.meta.env.VITE_JAZZ_APP_ID ?? "bluesky-offline-react-v2";
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
const sessionCacheKey = `${appId}:session`;

type Session = { did: string; token: string };

function readCachedSession(): Session | undefined {
  try {
    const value = JSON.parse(localStorage.getItem(sessionCacheKey) ?? "null") as Partial<Session> | null;
    return typeof value?.did === "string" && typeof value.token === "string" ? value as Session : undefined;
  } catch {
    return undefined;
  }
}

function cacheSession(session: Session) {
  localStorage.setItem(sessionCacheKey, JSON.stringify(session));
}

function signOut() {
  localStorage.removeItem(sessionCacheKey);
  return fetch("/api/auth/logout", { method: "POST" }).finally(() => location.reload());
}

function JazzApp({ session }: { session: Session }) {
  const config: DbConfig = {
    appId,
    serverUrl,
    env: "dev",
    userBranch: "main",
    jwtToken: session.token,
  };
  return <JazzProvider config={config} fallback={<LoadingScreen label="Opening your local cache…" />}>
    <Timeline did={session.did} onSignOut={() => void signOut()} />
  </JazzProvider>;
}

export function App() {
  const cachedSession = useRef(readCachedSession()).current;
  const [session, setSession] = useState<Session | null | undefined>(cachedSession);
  const [handle, setHandle] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const [loginError, setLoginError] = useState<string | null>(null);

  useEffect(() => {
    let stopped = false;
    const refresh = async () => {
      try {
        const response = await fetch("/api/session");
        if (!response.ok) {
          if (!cachedSession && !stopped) setSession(null);
          return;
        }
        const next = await response.json() as Session;
        cacheSession(next);
        if (!stopped) setSession(next);
      } catch {
        if (!cachedSession && !stopped) setSession(null);
      }
    };
    void refresh();
    const timer = window.setInterval(refresh, 5 * 60_000);
    return () => {
      stopped = true;
      window.clearInterval(timer);
    };
  }, [cachedSession]);

  if (session === undefined) return <LoadingScreen label="Checking your session…" />;
  if (session) return <JazzApp session={session} />;

  async function beginLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoggingIn(true);
    setLoginError(null);
    try {
      const response = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ handle }),
      });
      const result = await response.json() as { url?: string; error?: string };
      if (!response.ok || !result.url) throw new Error(result.error ?? "Could not start sign-in");
      location.href = result.url;
    } catch (error) {
      setLoggingIn(false);
      setLoginError(error instanceof Error ? error.message : "Could not start sign-in");
    }
  }

  return <main className="login">
    <section className="login-intro">
      <div className="brand-lockup"><div className="brand-mark" aria-hidden="true">J</div><div><p className="eyebrow">Local-first ATProto</p><p className="wordmark">{"Jazz ❤️ Bluesky"}</p></div></div>
      <h1>Keep up, even when your connection can’t.</h1>
      <p>Jazz and Bluesky together: a familiar timeline that loads offline and lets you write whenever inspiration strikes.</p>
      <ul>
        <li><span aria-hidden="true">✓</span><div><strong>Read anywhere</strong><p>Your recent timeline is cached on this device.</p></div></li>
        <li><span aria-hidden="true">✓</span><div><strong>Write offline</strong><p>Posts and reactions queue locally and publish when you reconnect.</p></div></li>
        <li><span aria-hidden="true">✓</span><div><strong>Stay in control</strong><p>Your PDS remains the source of truth.</p></div></li>
      </ul>
    </section>
    <section className="login-card" aria-labelledby="login-title">
      <p className="eyebrow">Welcome</p>
      <h2 id="login-title">Connect your Bluesky account</h2>
      <p>Enter your handle to continue through ATProto OAuth at your PDS.</p>
      <form onSubmit={beginLogin} aria-busy={loggingIn}>
        <label htmlFor="handle">Bluesky handle</label>
        <input id="handle" value={handle} onChange={(event) => setHandle(event.target.value)} placeholder="you.bsky.social" autoCapitalize="none" autoCorrect="off" required />
        <button className="primary" type="submit" disabled={loggingIn || !handle.trim()}>{loggingIn ? <><span className="spinner" aria-hidden="true" />Opening ATProto OAuth…</> : "Continue with ATProto OAuth"}</button>
        {(loggingIn || loginError) && <p className={loginError ? "form-status error" : "form-status"} role="status" aria-live="polite">{loginError ?? "Opening secure sign-in…"}</p>}
      </form>
      <p className="oauth-note">{"Jazz ❤️ Bluesky never asks for your Bluesky password."}</p>
    </section>
  </main>;
}
