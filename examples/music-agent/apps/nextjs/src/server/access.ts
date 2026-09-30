import "server-only";
import type { AccountAuthError, Db } from "jazz-tools";
import { app } from "@/schema";
import { auth } from "@/src/lib/auth";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";

export class AccessError extends Error {
  constructor(
    message: string,
    readonly status: 401 | 403 | 404 | 503,
  ) {
    super(message);
  }
}

/**
 * A Db that reads as the signed-in user, under the same permissions as the
 * browser, so permissions.ts stays the only place ownership is decided.
 *
 * Routes the browser calls without a bearer token, such as the audio
 * element's range requests, carry only the Better Auth cookie: the server
 * mints the user's app JWT from it and admits the request with that token,
 * exactly as the Jazz server admits the browser.
 */
export async function userDb(request: Request): Promise<Db> {
  const minted = await auth.api.getToken({ headers: request.headers }).catch(() => null);
  if (!minted?.token) throw new AccessError("sign in required", 401);
  const client = await backendJazzClient();
  try {
    return await client.forRequest({ headers: { authorization: `Bearer ${minted.token}` } });
  } catch (error) {
    const code = accountAuthErrorCode(error);
    // Only the account registry's answer about this user is an auth failure.
    // A registry that can't be reached is a server problem; anything else
    // (such as our own token failing verification) is a bug, so it propagates.
    if (code === undefined) throw error;
    console.warn(`MusicAgent request admission failed: ${code}`);
    if (UNAVAILABLE.has(code)) throw new AccessError("workspace unavailable", 503);
    throw new AccessError("workspace not prepared", 401);
  }
}

/** Registry codes that say nothing about the user: the request didn't get an answer. */
const UNAVAILABLE = new Set([
  "account_request_timeout",
  "account_request_failed",
  "invalid_account_response",
]);

/** The code of the account registry's refusal. The backend loads its own copy of the class, so match the name. */
function accountAuthErrorCode(error: unknown): string | undefined {
  if (error instanceof Error && error.name === "AccountAuthError")
    return (error as AccountAuthError).code;
  return undefined;
}

/** Load a turn the user may read; any other turn is simply not found. */
export async function requireTurn(db: Db, turnId: string) {
  const turn = await db.one(app.turns.where({ id: turnId }), { tier: "global" });
  if (!turn) throw new AccessError("turn not found", 404);
  return turn;
}

export function errorResponse(error: unknown): Response {
  if (error instanceof AccessError)
    return Response.json({ error: error.message }, { status: error.status });
  throw error;
}
