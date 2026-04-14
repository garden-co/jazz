"use client";

import { useEffect, useState } from "react";
import { type DbConfig } from "jazz-tools";
import { JazzProvider, useDb } from "jazz-tools/react";
import { authClient } from "@/src/lib/auth-client";
import { LocalFirstProvider } from "./local-first-provider";

/**
 * Picks between the anonymous (local-first) path and the authenticated
 * (BetterAuth JWT) path based on session state. The anonymous path is
 * handled by delegating to `LocalFirstProvider` so the two modes share
 * no internal state.
 */
export function BetterAuthProvider({ children }: React.PropsWithChildren) {
  const { data: authSession, isPending } = authClient.useSession();

  if (isPending) return null;

  if (!authSession?.session) {
    return <LocalFirstProvider>{children}</LocalFirstProvider>;
  }

  return <AuthenticatedJazzProvider>{children}</AuthenticatedJazzProvider>;
}

function AuthenticatedJazzProvider({ children }: React.PropsWithChildren) {
  const [config, setConfig] = useState<DbConfig | null>(null);

  useEffect(() => {
    let cancelled = false;
    authClient.token().then(({ data, error }) => {
      if (cancelled) return;
      if (!error && data?.token) setConfig(buildConfig(data.token));
    });
    return () => {
      cancelled = true;
    };
  }, []);

  if (!config) return null;

  return (
    <JazzProvider config={config} fallback={<p>Loading...</p>}>
      <JwtRefresh>{children}</JwtRefresh>
    </JazzProvider>
  );
}

/**
 * Keeps the JazzProvider's JWT in sync with BetterAuth. When Jazz reports
 * the session as unauthenticated (e.g. token expired), mint a fresh JWT
 * and hand it back.
 */
function JwtRefresh({ children }: React.PropsWithChildren) {
  const db = useDb();

  useEffect(() => {
    let cancelled = false;
    const unsubscribe = db.onAuthChanged((state) => {
      if (state.status !== "unauthenticated") return;
      authClient.token().then(({ data, error }) => {
        if (cancelled) return;
        if (!error && data?.token) db.updateAuthToken(data.token);
      });
    });
    return () => {
      cancelled = true;
      unsubscribe?.();
    };
  }, [db]);

  return children;
}

function buildConfig(jwtToken: string): DbConfig {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  if (!appId) {
    throw new Error(
      "NEXT_PUBLIC_JAZZ_APP_ID is not set. The withJazz Next plugin injects this at dev time; in production, set it explicitly in your environment.",
    );
  }
  return {
    appId,
    serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "ws://localhost:1625",
    env: "dev",
    userBranch: "main",
    driver: { type: "memory" },
    jwtToken,
  };
}
