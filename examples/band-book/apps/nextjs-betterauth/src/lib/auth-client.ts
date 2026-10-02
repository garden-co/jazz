import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient();

/** Seconds of validity a cached token must still have to be reused. */
const TOKEN_MARGIN_SECONDS = 60;

type CachedToken = { principal: string; token: string; expiresAt: number };
let cached: CachedToken | null = null;
let inFlight: { principal: string; promise: Promise<string | null> } | null = null;

/** The `sub` and `exp` claims of a JWT, or nulls when it has none. */
function claims(token: string): { sub: string | null; exp: number } {
  try {
    const payload = token.split(".")[1]!.replace(/-/g, "+").replace(/_/g, "/");
    const { sub, exp } = JSON.parse(atob(payload)) as { sub?: unknown; exp?: unknown };
    return {
      sub: typeof sub === "string" ? sub : null,
      exp: typeof exp === "number" ? exp : 0,
    };
  } catch {
    return { sub: null, exp: 0 };
  }
}

async function fetchJwt(): Promise<string | null> {
  const response = await fetch("/api/auth/token", { credentials: "same-origin" });
  if (!response.ok) return null;
  const body = (await response.json()) as { token?: unknown };
  return typeof body.token === "string" ? body.token : null;
}

/**
 * A Better Auth JWT for server calls made by `principal`, the subject of the
 * signed-in Jazz session (the Better Auth user id). The token is reused while
 * it was minted for that same user and has more than a minute left, so a page
 * that makes several server calls fetches it once. Concurrent callers share
 * one request. A token is only ever handed to the user it names: after a
 * sign-out the Jazz session ends, and another user's calls ask for their own.
 */
export async function getJwtFromBetterAuth(principal: string): Promise<string | null> {
  const now = Date.now() / 1000;
  if (cached?.principal === principal && cached.expiresAt - TOKEN_MARGIN_SECONDS > now)
    return cached.token;
  if (inFlight?.principal === principal) return await inFlight.promise;

  const promise = fetchJwt();
  inFlight = { principal, promise };
  try {
    const token = await promise;
    const { sub, exp } = token ? claims(token) : { sub: null, exp: 0 };
    cached = token && sub === principal ? { principal, token, expiresAt: exp } : null;
    return token;
  } finally {
    if (inFlight?.promise === promise) inFlight = null;
  }
}
