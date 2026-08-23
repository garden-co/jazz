"use client";
import { useEffect, useState } from "react";
import { JazzProvider as BaseJazzProvider, useDb } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { authClient } from "@/src/lib/auth-client";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

export function JazzProvider({ children }: { children: React.ReactNode }) {
  const [config, setConfig] = useState<DbConfig | null>(null);
  useEffect(() => {
    let cancelled = false;
    void authClient.token().then((result: { data?: { token?: string } | null }) => {
      const token = result.data?.token;
      if (!cancelled && token && appId && serverUrl)
        setConfig({ appId, serverUrl, jwtToken: token });
    });
    return () => {
      cancelled = true;
    };
  }, []);
  if (!config) return <p className="shell">Connecting BandChat…</p>;
  return (
    <BaseJazzProvider config={config} fallback={null}>
      <JwtRefresh />
      {children}
    </BaseJazzProvider>
  );
}

function JwtRefresh() {
  const db = useDb();
  useEffect(
    () =>
      db.onAuthChanged((state) => {
        if (state.error === "expired")
          void authClient.token().then((result: { data?: { token?: string } | null }) => {
            const token = result.data?.token;
            if (token) db.updateAuthToken(token);
          });
      }),
    [db],
  );
  return null;
}
