"use client";

import { useEffect, useMemo, useState } from "react";
import type { DbConfig } from "jazz-tools";
import { JazzProvider as JazzBaseProvider, useDb, useLocalFirstAuth } from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

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
 * Jazz provider for the local-first + BetterAuth starter. Watches the
 * Better Auth session and builds the appropriate DbConfig — an anonymous
 * local-first secret when there's no session, a Better Auth JWT when
 * there is.
 */
export function JazzProvider({ children }: React.PropsWithChildren) {
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
      const missing = [
        !APP_ID && "NEXT_PUBLIC_JAZZ_APP_ID",
        !SERVER_URL && "NEXT_PUBLIC_JAZZ_SERVER_URL",
      ]
        .filter((v) => !!v)
        .join(" & ");
      throw new Error(
        `${missing} not set. The withJazz Next plugin injects these at dev time; in production, set them explicitly in your environment.`,
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
      <JwtRefresh />
      {children}
    </JazzBaseProvider>
  );
}
