import "server-only";
import type { Db } from "jazz-tools";
import { app } from "@/schema";
import { auth } from "@/src/lib/auth";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";

export class AccessError extends Error {
  constructor(
    message: string,
    readonly status: 401 | 403 | 404,
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
  try {
    return await (
      await backendJazzClient()
    ).forRequest({ headers: { authorization: `Bearer ${minted.token}` } });
  } catch (error) {
    console.warn("MusicAgent request admission failed", error);
    throw new AccessError("workspace not prepared", 401);
  }
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
