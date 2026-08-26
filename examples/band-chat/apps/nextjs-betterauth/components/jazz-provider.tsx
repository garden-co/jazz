"use client";

import { useEffect, useState } from "react";
import { JazzProvider as JazzBaseProvider, useDb } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = authClient.useSession();
  const [connection, setConnection] = useState<{
    config: DbConfig;
    principal: string;
  } | null>(null);

  useEffect(() => {
    const principal = session?.user.id;
    if (!principal) {
      setConnection(null);
      return;
    }
    if (!appId || !serverUrl) throw new Error("withJazz must provide the public app configuration");

    // Do not leave the previous principal's Jazz context mounted while the
    // replacement token is in flight.
    setConnection(null);
    let cancelled = false;
    void getJwtFromBetterAuth().then((jwtToken) => {
      if (!cancelled && jwtToken) {
        setConnection({ config: { appId, serverUrl, jwtToken }, principal });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [session?.user.id]);

  if (!connection) return <p className="loading-state">Connecting BandChat…</p>;
  return (
    <JazzBaseProvider
      config={connection.config}
      fallback={<p className="loading-state">Opening rooms…</p>}
      key={connection.principal}
    >
      <JwtRefresh />
      {children}
    </JazzBaseProvider>
  );
}

function JwtRefresh() {
  const db = useDb();
  useEffect(
    () =>
      db.onAuthChanged((state) => {
        if (state.error !== "expired") return;
        void getJwtFromBetterAuth().then((token) => {
          if (token) db.updateAuthToken(token);
        });
      }),
    [db],
  );
  return null;
}
