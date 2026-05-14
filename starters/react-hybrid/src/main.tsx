import { StrictMode, useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider as JazzBaseProvider, useDb, useLocalFirstAuth } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { authClient } from "./auth-client";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

function JwtRefresh() {
  const db = useDb();
  useEffect(
    () =>
      db.onAuthChanged((state) => {
        if (state.error !== "expired") return;
        authClient.token().then(({ data, error }) => {
          if (!error && data?.token) db.updateAuthToken(data.token);
        });
      }),
    [db],
  );
  return null;
}

/**
 * Hybrid provider: renders JazzProvider regardless of BetterAuth session state.
 * Anonymous visitors get a local-first identity (from useLocalFirstAuth);
 * signed-in users get a BetterAuth-issued JWT. Switching between the two
 * triggers JazzProvider to rebuild against the new config.
 */
function HybridProvider({ children }: React.PropsWithChildren) {
  const { data: authSession, isPending } = authClient.useSession();
  const { secret, isLoading: secretLoading } = useLocalFirstAuth();
  const [jwtToken, setJwtToken] = useState<string | null>(null);
  const authenticated = Boolean(authSession?.session);

  useEffect(() => {
    if (!authenticated) {
      setJwtToken(null);
      return;
    }
    let cancelled = false;
    authClient.token().then(({ data, error }) => {
      if (cancelled) return;
      if (!error && data?.token) setJwtToken(data.token);
    });
    return () => {
      cancelled = true;
    };
  }, [authenticated]);

  const config = useMemo<DbConfig | null>(() => {
    if (!APP_ID || !SERVER_URL) {
      const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
        .filter((v) => !!v)
        .join(" & ");
      throw new Error(
        `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
      );
    }
    if (authenticated) {
      return jwtToken ? { appId: APP_ID, serverUrl: SERVER_URL, jwtToken } : null;
    }
    if (secretLoading || !secret) return null;
    return { appId: APP_ID, serverUrl: SERVER_URL, secret };
  }, [authenticated, jwtToken, secret, secretLoading]);

  if (isPending || !config) return null;

  return (
    <JazzBaseProvider config={config} fallback={<p>Loading...</p>}>
      {authenticated && <JwtRefresh />}
      {children}
    </JazzBaseProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <HybridProvider>
      <App />
    </HybridProvider>
  </StrictMode>,
);
