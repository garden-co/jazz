import { after } from "next/server";
import { queueAssistantTurn, runTurn } from "@/src/agent/runner";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { errorResponse, requireTurn, userDb } from "@/src/server/access";

export const runtime = "nodejs";

/**
 * Ask the agent to answer a user turn the browser already wrote to Jazz. The
 * reply is generated after the response is sent; the browser only watches the
 * turn's row, so it can close the tab and come back to the finished answer.
 */
export async function POST(request: Request) {
  try {
    const db = await userDb(request);
    const { userTurnId } = (await request.json()) as { userTurnId?: string };
    if (!userTurnId) return Response.json({ error: "userTurnId required" }, { status: 400 });
    const userTurn = await requireTurn(db, userTurnId);
    if (userTurn.role !== "user")
      return Response.json({ error: "only user turns get replies" }, { status: 400 });
    // Idempotent: a retried request gets the reply that already exists. Finding
    // it, queueing a new one and moving the head are one exclusive transaction.
    const { turnId } = await queueAssistantTurn(
      (await backendJazzClient()).db,
      userTurn.conversationId,
      userTurnId,
      { reuse: true },
    );
    // Claiming is idempotent too: a reply that is already running or done is left alone.
    after(() => runTurn(turnId));
    return Response.json({ turnId });
  } catch (error) {
    return errorResponse(error);
  }
}
