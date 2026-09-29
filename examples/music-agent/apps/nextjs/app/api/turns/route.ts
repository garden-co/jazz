import { after } from "next/server";
import { app } from "@/schema";
import { queueAssistantTurn, runTurn } from "@/src/agent/runner";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { errorResponse, requireAccount, requireTurn } from "@/src/server/access";

export const runtime = "nodejs";

/**
 * Ask the agent to answer a user turn the browser already wrote to Jazz. The
 * reply is generated after the response is sent; the browser only watches the
 * turn's row, so it can close the tab and come back to the finished answer.
 */
export async function POST(request: Request) {
  try {
    const accountId = await requireAccount(request);
    const { userTurnId } = (await request.json()) as { userTurnId?: string };
    if (!userTurnId) return Response.json({ error: "userTurnId required" }, { status: 400 });
    const userTurn = await requireTurn(accountId, userTurnId);
    if (userTurn.role !== "user")
      return Response.json({ error: "only user turns get replies" }, { status: 400 });
    const db = (await backendJazzClient()).db;
    // Idempotent: a retried request returns the reply that already exists.
    const existing = await db.one(
      app.turns.where({ conversationId: userTurn.conversationId, parentId: userTurnId }),
      { tier: "global" },
    );
    if (existing) return Response.json({ turnId: existing.id });
    const turnId = queueAssistantTurn(db, userTurn.conversationId, userTurnId);
    after(() => runTurn(turnId));
    return Response.json({ turnId });
  } catch (error) {
    return errorResponse(error);
  }
}
