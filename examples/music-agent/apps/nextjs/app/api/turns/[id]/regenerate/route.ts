import { after } from "next/server";
import { queueAssistantTurn, runTurn } from "@/src/agent/runner";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { errorResponse, requireAccount, requireTurn } from "@/src/server/access";

export const runtime = "nodejs";

/** Answer the same user turn again. The new reply is a sibling branch; the old one stays. */
export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const accountId = await requireAccount(request);
    const turn = await requireTurn(accountId, (await params).id);
    if (turn.role !== "assistant" || !turn.parentId)
      return Response.json({ error: "only replies can be regenerated" }, { status: 400 });
    const db = (await backendJazzClient()).db;
    const turnId = queueAssistantTurn(db, turn.conversationId, turn.parentId);
    after(() => runTurn(turnId));
    return Response.json({ turnId });
  } catch (error) {
    return errorResponse(error);
  }
}
