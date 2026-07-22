import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { useEffect, useRef, useState } from "react";
import { jazzAppId } from "../shared/identifiers.js";
import { LoginView } from "./components/LoginView.js";
import { LoadingScreen } from "./components/TimelineView.js";
import { Timeline } from "./Timeline.js";
import {
  keepMountedSession,
  refreshAuthentication,
  type AuthenticationState,
  type JazzCredentials,
} from "./model/auth-state.js";

const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
const sessionCacheKey = `${jazzAppId}:session`;

function readCachedSession(): JazzCredentials | undefined {
  try {
    const value = JSON.parse(
      localStorage.getItem(sessionCacheKey) ?? "null",
    ) as Partial<JazzCredentials> | null;
    return typeof value?.did === "string" && typeof value.token === "string"
      ? { did: value.did, token: value.token }
      : undefined;
  } catch {
    return undefined;
  }
}

function cacheSession(session: JazzCredentials) {
  localStorage.setItem(sessionCacheKey, JSON.stringify(session));
}

async function signOut() {
  localStorage.removeItem(sessionCacheKey);
  try {
    await fetch("/api/auth/logout", { method: "POST" });
  } finally {
    location.reload();
  }
}

function JazzApp({
  session,
  onJWTExpired,
}: {
  session: JazzCredentials;
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
      autoAttachDevTools={false}
      fallback={<LoadingScreen label="Opening your local cache…" />}
      onJWTExpired={onJWTExpired}
    >
      <Timeline did={session.did} onSignOut={signOut} />
    </JazzProvider>
  );
}

export function App() {
  const cachedCredentials = useRef(readCachedSession()).current;
  const latestCredentials = useRef(cachedCredentials);
  const [authentication, setAuthentication] = useState<AuthenticationState>(
    cachedCredentials ? { kind: "signed-in", session: cachedCredentials } : { kind: "checking" },
  );

  function applyAuthentication(next: AuthenticationState) {
    if (next.kind === "signed-in") {
      latestCredentials.current = next.session;
      cacheSession(next.session);
      // JazzProvider owns the OPFS connection. Keep it mounted while its JWT rotates.
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
    latestCredentials.current = undefined;
    setAuthentication(next);
  }

  function requestAuthentication() {
    return refreshAuthentication(
      latestCredentials.current,
      () => fetch("/api/session"),
      () => localStorage.removeItem(sessionCacheKey),
    );
  }

  async function refreshSession() {
    const next = await requestAuthentication();
    applyAuthentication(next);
    return next;
  }

  async function refreshJazzToken() {
    const next = await refreshSession();
    return next.kind === "signed-in" ? next.session.token : null;
  }

  useEffect(() => {
    let stopped = false;
    const refresh = async () => {
      const next = await requestAuthentication();
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
  return (
    <LoginView
      sessionError={authentication.kind === "unavailable" ? authentication.message : undefined}
    />
  );
}
