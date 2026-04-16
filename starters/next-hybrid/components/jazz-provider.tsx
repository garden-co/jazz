"use client";

import { useEffect, useState } from "react";
import { type DbConfig, BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider as JazzBaseProvider, useDb } from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";

function JwtRefresh() {
  const db = useDb();
  useEffect(
    () =>
      db.onAuthChanged((state) => {
        if (state.status !== "unauthenticated") return;
        authClient.token().then(({ data, error }) => {
          if (!error && data?.token) db.updateAuthToken(data.token);
        });
      }),
    [db],
  );
  return null;
}

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

/**
 * Jazz provider for the local-first + BetterAuth starter. Watches the
 * Better Auth session and builds the appropriate DbConfig — an anonymous
 * local-first secret when there's no session, a Better Auth JWT when
 * there is.
 */
export function JazzProvider({ children }: React.PropsWithChildren) {
  const { data: authSession, isPending } = authClient.useSession();
  const [config, setConfig] = useState<DbConfig | null>(null);
  const authenticated = Boolean(authSession?.session);

  useEffect(() => {
    if (isPending) return;
    let cancelled = false;
    setConfig(null);

    (async () => {
      const next = authenticated ? await buildJwtConfig() : await buildLocalFirstConfig();
      if (!cancelled && next) setConfig(next);
    })();

    return () => {
      cancelled = true;
    };
  }, [isPending, authenticated]);

  if (isPending || !config) return null;

  return (
    <JazzBaseProvider config={config} fallback={<p>Loading...</p>}>
      <JwtRefresh />
      {children}
    </JazzBaseProvider>
  );
}

function baseConfig(): Omit<DbConfig, "jwtToken" | "auth"> {
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
  return {
    appId: APP_ID,
    serverUrl: SERVER_URL,
    // The persistent driver's worker currently fails to load under
    // Next.js + Turbopack dev (jaz-ad1f); memory mode avoids it.
    driver: { type: "memory" },
  };
}

async function buildLocalFirstConfig(): Promise<DbConfig> {
  const secret = await BrowserAuthSecretStore.getOrCreateSecret();
  return { ...baseConfig(), auth: { localFirstSecret: secret } };
}

async function buildJwtConfig(): Promise<DbConfig | null> {
  const { data, error } = await authClient.token();
  if (error || !data?.token) return null;
  return { ...baseConfig(), jwtToken: data.token };
}
