import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient();

/** Seconds of validity a cached token must still have to be reused. */
const TOKEN_MARGIN_SECONDS = 60;

type CachedToken = { sessionKey: string; token: string; expiresAt: number };
let cached: CachedToken | null = null;
let inFlight: { sessionKey: string; promise: Promise<string | null> } | null = null;

/** The signed-in Better Auth user and session, or null when signed out. */
function currentSessionKey(): string | null {
  const data = authClient.$store.atoms.session?.get().data as
    | { user: { id: string }; session: { id: string } }
    | null
    | undefined;
  return data ? `${data.user.id}:${data.session.id}` : null;
}

/** The `exp` claim of a JWT in seconds, or 0 when it has none. */
function expiresAt(token: string): number {
  try {
    const payload = token.split(".")[1]!.replace(/-/g, "+").replace(/_/g, "/");
    const { exp } = JSON.parse(atob(payload)) as { exp?: unknown };
    return typeof exp === "number" ? exp : 0;
  } catch {
    return 0;
  }
}

async function fetchJwt(): Promise<string | null> {
  const response = await fetch("/api/auth/token", { credentials: "same-origin" });
  if (!response.ok) return null;
  const body = (await response.json()) as { token?: unknown };
  return typeof body.token === "string" ? body.token : null;
}

/**
 * A Better Auth JWT for server calls. The token is reused while the same
 * Better Auth session is signed in and it has more than a minute left, so a
 * page that makes several server calls fetches it once. Concurrent callers
 * share one request. A different session (sign-out, another account) never
 * sees the previous session's token.
 */
export async function getJwtFromBetterAuth(): Promise<string | null> {
  const sessionKey = currentSessionKey();
  const now = Date.now() / 1000;
  if (
    sessionKey &&
    cached?.sessionKey === sessionKey &&
    cached.expiresAt - TOKEN_MARGIN_SECONDS > now
  )
    return cached.token;
  if (sessionKey && inFlight?.sessionKey === sessionKey) return await inFlight.promise;

  const promise = fetchJwt();
  if (sessionKey) inFlight = { sessionKey, promise };
  try {
    const token = await promise;
    cached = token && sessionKey ? { sessionKey, token, expiresAt: expiresAt(token) } : null;
    return token;
  } finally {
    if (inFlight?.promise === promise) inFlight = null;
  }
}
