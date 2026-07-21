import { Button, Card, Spinner, TextField } from "@radix-ui/themes";
import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { useEffect, useRef, useState, type FormEvent } from "react";
import { jazzAppId } from "../shared/identifiers.js";
import { SuccessIcon } from "./Icons.js";
import { Timeline } from "./Timeline.js";
import { LoadingScreen } from "./TimelineView.js";
import {
  keepMountedSession,
  refreshAuthentication,
  type AuthenticationState,
  type Session,
} from "./auth-state.js";

const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
const sessionCacheKey = `${jazzAppId}:session`;

function readCachedSession(): Session | undefined {
  try {
    const value = JSON.parse(localStorage.getItem(sessionCacheKey) ?? "null") as Partial<Session> | null;
    return typeof value?.did === "string" && typeof value.token === "string"
      ? { did: value.did, token: value.token }
      : undefined;
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

function JazzApp({ session, onJWTExpired }: {
  session: Session;
  onJWTExpired: () => Promise<string | null>;
}) {
  const config: DbConfig = {
    appId: jazzAppId,
    serverUrl,
    env: "dev",
    userBranch: "main",
    jwtToken: session.token,
  };
  return (
    <JazzProvider
      config={config}
      fallback={<LoadingScreen label="Opening your local cache…" />}
      onJWTExpired={onJWTExpired}
    >
      <Timeline did={session.did} onSignOut={signOut} />
    </JazzProvider>
  );
}

export function App() {
  const cachedSession = useRef(readCachedSession()).current;
  const currentSession = useRef(cachedSession);
  const [authentication, setAuthentication] = useState<AuthenticationState>(
    cachedSession ? { kind: "signed-in", session: cachedSession } : { kind: "checking" },
  );
  const [handle, setHandle] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const [loginError, setLoginError] = useState<string | null>(null);

  function applyAuthentication(next: AuthenticationState) {
    if (next.kind === "signed-in") {
      currentSession.current = next.session;
      cacheSession(next.session);
      // Keep the provider config stable so refreshing a JWT never reopens the OPFS cache.
      setAuthentication((current) => {
        const mounted = keepMountedSession(
          current.kind === "signed-in" ? current.session : undefined,
          next.session,
        );
        return current.kind === "signed-in" && current.session === mounted
          ? current
          : { kind: "signed-in", session: mounted };
      });
      return;
    }
    currentSession.current = undefined;
    setAuthentication(next);
  }

  async function refreshSession() {
    const next = await refreshAuthentication(
      currentSession.current,
      () => fetch("/api/session"),
      () => localStorage.removeItem(sessionCacheKey),
    );
    applyAuthentication(next);
    return next;
  }

  async function refreshJazzToken() {
    const previousToken = currentSession.current?.token;
    const next = await refreshSession();
    return next.kind === "signed-in" && next.session.token !== previousToken
      ? next.session.token
      : null;
  }

  useEffect(() => {
    let stopped = false;
    const refresh = async () => {
      const next = await refreshAuthentication(
        currentSession.current,
        () => fetch("/api/session"),
        () => localStorage.removeItem(sessionCacheKey),
      );
      if (!stopped) applyAuthentication(next);
    };
    refresh();
    const timer = window.setInterval(refresh, 5 * 60_000);
    return () => {
      stopped = true;
      window.clearInterval(timer);
    };
  }, []);

  if (authentication.kind === "checking") {
    return <LoadingScreen label="Checking your session…" />;
  }
  if (authentication.kind === "signed-in") {
    return <JazzApp session={authentication.session} onJWTExpired={refreshJazzToken} />;
  }

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

  return (
    <main className="login">
      <section className="login-intro">
        <div className="brand-lockup">
          <div className="brand-mark" aria-hidden="true">J</div>
          <div>
            <p className="eyebrow">Local-first ATProto</p>
            <p className="wordmark">Jazz ❤️ Bluesky</p>
          </div>
        </div>
        <h1>Keep up, even when your connection can’t.</h1>
        <p>
          Jazz and Bluesky together: a familiar timeline that loads offline and
          lets you write whenever inspiration strikes.
        </p>
        <ul>
          <li>
            <span><SuccessIcon /></span>
            <div>
              <strong>Read anywhere</strong>
              <p>Your recent timeline is cached on this device.</p>
            </div>
          </li>
          <li>
            <span><SuccessIcon /></span>
            <div>
              <strong>Write offline</strong>
              <p>Posts and reactions queue locally and publish when you reconnect.</p>
            </div>
          </li>
          <li>
            <span><SuccessIcon /></span>
            <div>
              <strong>Stay in control</strong>
              <p>Your PDS remains the source of truth.</p>
            </div>
          </li>
        </ul>
      </section>
      <Card asChild size="4">
        <section className="login-card" aria-labelledby="login-title">
          <p className="eyebrow">Welcome</p>
          <h2 id="login-title">Connect your Bluesky account</h2>
          <p>Enter your handle to continue through ATProto OAuth at your PDS.</p>
          <form onSubmit={beginLogin} aria-busy={loggingIn}>
            <label htmlFor="handle">Bluesky handle</label>
            <TextField.Root
              id="handle"
              size="3"
              value={handle}
              onChange={(event) => setHandle(event.target.value)}
              placeholder="you.bsky.social"
              autoCapitalize="none"
              autoCorrect="off"
              required
            />
            <Button
              type="submit"
              size="3"
              disabled={loggingIn || !handle.trim()}
            >
              {loggingIn ? (
                <>
                  <Spinner aria-hidden="true" />
                  Opening ATProto OAuth…
                </>
              ) : "Continue with ATProto OAuth"}
            </Button>
            {(loggingIn || loginError) && (
              <p
                className={loginError ? "form-status error" : "form-status"}
                role="status"
                aria-live="polite"
              >
                {loginError ?? "Opening secure sign-in…"}
              </p>
            )}
          </form>
          <p className="oauth-note">
            Jazz ❤️ Bluesky never asks for your Bluesky password.
          </p>
        </section>
      </Card>
    </main>
  );
}
