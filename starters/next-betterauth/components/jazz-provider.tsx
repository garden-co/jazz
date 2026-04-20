"use client";

import { useEffect, useState } from "react";
import { JazzProvider as JazzBaseProvider, useDb } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { authClient } from "@/lib/auth-client";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = authClient.useSession();
  const [config, setConfig] = useState<DbConfig | null>(null);

  useEffect(() => {
    if (!session) {
      setConfig(null);
      return;
    }
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
    let cancelled = false;
    authClient.token().then(({ data, error }) => {
      if (cancelled || error || !data?.token) return;
      setConfig({ appId: APP_ID, serverUrl: SERVER_URL, jwtToken: data.token });
    });
    return () => {
      cancelled = true;
    };
  }, [session]);

  if (!session) return <>{children}</>;
  if (!config) return null;

  return (
    <JazzBaseProvider config={config} fallback={null}>
      <JwtRefresh />
      {children}
    </JazzBaseProvider>
  );
}

// Component for refreshing the JWT when it expires.
// Separated out so we can use `useDb` from inside the provider.
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
