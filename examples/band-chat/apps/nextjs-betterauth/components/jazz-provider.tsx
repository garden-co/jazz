"use client";

import { useEffect, useState } from "react";
import { JazzProvider as JazzBaseProvider, useDb } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = authClient.useSession();
  const [config, setConfig] = useState<DbConfig | null>(null);

  useEffect(() => {
    if (!session) {
      setConfig(null);
      return;
    }
    if (!appId || !serverUrl) throw new Error("withJazz must provide the public app configuration");

    let cancelled = false;
    void getJwtFromBetterAuth().then((jwtToken) => {
      if (!cancelled && jwtToken) setConfig({ appId, serverUrl, jwtToken });
    });
    return () => {
      cancelled = true;
    };
  }, [session]);

  if (!config) return <p className="loading-state">Connecting BandChat…</p>;
  return (
    <JazzBaseProvider config={config} fallback={<p className="loading-state">Opening rooms…</p>}>
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
