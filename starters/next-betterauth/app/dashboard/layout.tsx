"use client";

import { useEffect, useState } from "react";
import { JazzProvider, useDb } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { authClient } from "@/src/lib/auth-client";

/**
 * Returns the Jazz app ID or throws a clear error. Called lazily from the
 * component body (not at module load) so Next's static prerender can still
 * compile the module even when the env var is unset — the throw fires at
 * actual render time, when the user hits /dashboard.
 */
function requireAppId(): string {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  if (!appId) {
    throw new Error(
      "NEXT_PUBLIC_JAZZ_APP_ID is not set. The withJazz Next plugin injects this at dev time; in production, set it explicitly in your environment.",
    );
  }
  return appId;
}

const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "ws://localhost:1625";

/**
 * Cache the JWT at module scope so navigating between dashboard routes
 * reuses the existing token instead of blank-rendering for a fresh fetch
 * on every remount. Cleared on full page reload, which is when we want a
 * new token anyway.
 */
let cachedJwt: string | null = null;
let inflight: Promise<string | null> | null = null;

function loadJwt(): Promise<string | null> {
  if (cachedJwt) return Promise.resolve(cachedJwt);
  if (inflight) return inflight;
  inflight = authClient
    .token()
    .then(({ data, error }) => {
      inflight = null;
      if (error || !data?.token) return null;
      cachedJwt = data.token;
      return cachedJwt;
    })
    .catch(() => {
      inflight = null;
      return null;
    });
  return inflight;
}

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  const [jwtToken, setJwtToken] = useState<string | null>(cachedJwt);

  useEffect(() => {
    if (jwtToken) return;
    let cancelled = false;
    loadJwt().then((token) => {
      if (!cancelled) setJwtToken(token);
    });
    return () => {
      cancelled = true;
    };
  }, [jwtToken]);

  if (!jwtToken) return null;

  const config: DbConfig = {
    appId: requireAppId(),
    serverUrl: SERVER_URL,
    env: "dev",
    userBranch: "main",
    driver: { type: "memory" },
    jwtToken,
  };

  return (
    <JazzProvider config={config} fallback={null}>
      <JwtRefresh>{children}</JwtRefresh>
    </JazzProvider>
  );
}

/**
 * Keeps the JazzProvider's JWT in sync with BetterAuth. When Jazz reports
 * the session as unauthenticated (e.g. token expired), mint a fresh JWT
 * and hand it back. Mirrors the pattern used in next-localfirst.
 */
function JwtRefresh({ children }: React.PropsWithChildren) {
  const db = useDb();

  useEffect(() => {
    return db.onAuthChanged((state) => {
      if (state.status !== "unauthenticated") return;
      cachedJwt = null;
      loadJwt().then((token) => {
        if (token) db.updateAuthToken(token);
      });
    });
  }, [db]);

  return children;
}
