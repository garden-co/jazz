import { after } from "next/server";
import { runTurn } from "@/src/agent/runner";
import { errorResponse, requireAccount, requireTurn } from "@/src/server/access";

export const runtime = "nodejs";

/** Continue an interrupted or failed reply in place, from where it stopped. */
export async function POST(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const accountId = await requireAccount(request);
    const turn = await requireTurn(accountId, (await params).id);
    if (turn.status !== "interrupted" && turn.status !== "failed")
      return Response.json({ error: `turn is ${turn.status}` }, { status: 409 });
    after(() => runTurn(turn.id));
    return Response.json({ turnId: turn.id });
  } catch (error) {
    return errorResponse(error);
  }
}
