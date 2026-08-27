"use client";

import { useEffect, useState } from "react";
import { JazzProvider as JazzBaseProvider, useDb } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "poster-shop-local";
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200";

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const { data: session } = authClient.useSession();
  const principal = session?.user.id ?? null;
  const [connection, setConnection] = useState<{ config: DbConfig; principal: string } | null>(
    null,
  );
  useEffect(() => {
    if (!principal) return void setConnection(null);
    setConnection(null);
    let cancelled = false;
    void getJwtFromBetterAuth().then((jwtToken) => {
      if (!cancelled && jwtToken)
        setConnection({ config: { appId, serverUrl, jwtToken }, principal });
    });
    return () => {
      cancelled = true;
    };
  }, [principal]);
  if (!principal || !connection || connection.principal !== principal) return <>{children}</>;
  return (
    <JazzBaseProvider
      config={connection.config}
      key={principal}
      fallback={<p>Opening poster studio…</p>}
    >
      <JwtRefresh principal={principal} />
      {children}
    </JazzBaseProvider>
  );
}

function JwtRefresh({ principal }: { principal: string }) {
  const db = useDb();
  const { data: session } = authClient.useSession();
  useEffect(
    () =>
      db.onAuthChanged((state) => {
        if (state.error !== "expired" || session?.user.id !== principal) return;
        void getJwtFromBetterAuth().then((token) => token && db.updateAuthToken(token));
      }),
    [db, principal, session?.user.id],
  );
  return null;
}
