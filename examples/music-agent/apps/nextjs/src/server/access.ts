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
 * Resolve the Jazz account behind a request from its Better Auth cookie. The
 * bootstrap route verified the account once (from the Jazz session token) and
 * stored the link in `profiles`, so routes the browser calls without a bearer
 * token, such as the audio element's range requests, can still authorize.
 */
export async function requireAccount(request: Request): Promise<string> {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) throw new AccessError("sign in required", 401);
  const db = (await backendJazzClient()).db;
  const profile = await db.one(app.profiles.where({ authUserId: session.user.id }), {
    tier: "global",
  });
  if (!profile) throw new AccessError("workspace not prepared", 401);
  return profile.accountId;
}

/** Load a conversation only if the account owns it. */
export async function requireConversation(accountId: string, conversationId: string) {
  const db = (await backendJazzClient()).db;
  const conversation = await db.one(app.conversations.where({ id: conversationId }), {
    tier: "global",
  });
  if (!conversation || conversation.ownerAccount !== accountId)
    throw new AccessError("conversation not found", 404);
  return conversation;
}

/** Load a turn only if the account owns its conversation. */
export async function requireTurn(accountId: string, turnId: string) {
  const db = (await backendJazzClient()).db;
  const turn = await db.one(app.turns.where({ id: turnId }), { tier: "global" });
  if (!turn) throw new AccessError("turn not found", 404);
  await requireConversation(accountId, turn.conversationId);
  return turn;
}

export function errorResponse(error: unknown): Response {
  if (error instanceof AccessError)
    return Response.json({ error: error.message }, { status: error.status });
  throw error;
}
