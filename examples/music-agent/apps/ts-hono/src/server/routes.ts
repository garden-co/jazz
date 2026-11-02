import { app } from "@/schema";
import { agentLabel } from "@/src/agent/config";
import { queueAssistantTurn, runTurn } from "@/src/agent/runner";
import { auth } from "@/src/lib/auth";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { errorResponse, requireTurn, userDb } from "./access";
import { bootstrapWorkspace } from "./bootstrap";

/**
 * The agent's HTTP routes, as plain `Request -> Response` handlers mounted by
 * server/index.ts. Replies are generated after the response is sent; the
 * browser only watches the turn's row in Jazz, so it can close the tab and
 * come back to the finished answer.
 */

/** Run a reply in the background, after the response that queued it. */
function inBackground(turnId: string) {
  setImmediate(() => {
    runTurn(turnId).catch((error) => console.warn("MusicAgent reply failed", error));
  });
}

/** Which agent answers, so the browser can label replies. */
export function config(): Response {
  return Response.json({ agentLabel: agentLabel() });
}

/** First open: find the Jazz account behind the Better Auth user, then seed. */
export async function bootstrap(request: Request): Promise<Response> {
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session?.user) return Response.json({ error: "sign in required" }, { status: 401 });
  const account = await (await backendJazzClient()).accountFor(request);
  if (!account) return Response.json({ error: "account required" }, { status: 401 });
  const firstReply = await bootstrapWorkspace(account, session.user.id, session.user.name);
  if (firstReply) inBackground(firstReply);
  return Response.json({ ok: true });
}

/** Ask the agent to answer a user turn the browser already wrote to Jazz. */
export async function requestReply(request: Request): Promise<Response> {
  try {
    const db = await userDb(request);
    const { userTurnId } = (await request.json()) as { userTurnId?: string };
    if (!userTurnId) return Response.json({ error: "userTurnId required" }, { status: 400 });
    const userTurn = await requireTurn(db, userTurnId);
    if (userTurn.role !== "user")
      return Response.json({ error: "only user turns get replies" }, { status: 400 });
    // Idempotent: a retried request gets the reply that already exists. Finding
    // it, queueing a new one and moving the head are one serializable transaction.
    const { turnId } = await queueAssistantTurn(
      (await backendJazzClient()).db,
      userTurn.conversationId,
      userTurnId,
      { reuse: true },
    );
    // Claiming is idempotent too: a reply that is already running or done is left alone.
    inBackground(turnId);
    return Response.json({ turnId });
  } catch (error) {
    return errorResponse(error);
  }
}

/** Answer the same user turn again. The new reply is a sibling branch; the old one stays. */
export async function regenerate(request: Request, id: string): Promise<Response> {
  try {
    const turn = await requireTurn(await userDb(request), id);
    if (turn.role !== "assistant" || !turn.parentId)
      return Response.json({ error: "only replies can be regenerated" }, { status: 400 });
    const { turnId } = await queueAssistantTurn(
      (await backendJazzClient()).db,
      turn.conversationId,
      turn.parentId,
    );
    inBackground(turnId);
    return Response.json({ turnId });
  } catch (error) {
    return errorResponse(error);
  }
}

/** Continue an interrupted or failed reply in place, from where it stopped. */
export async function resume(request: Request, id: string): Promise<Response> {
  try {
    const turn = await requireTurn(await userDb(request), id);
    if (turn.role !== "assistant" || (turn.status !== "interrupted" && turn.status !== "failed"))
      return Response.json({ error: `turn is ${turn.status}` }, { status: 409 });
    inBackground(turn.id);
    return Response.json({ turnId: turn.id });
  } catch (error) {
    return errorResponse(error);
  }
}

/** Largest page served per request; the audio element asks for the rest as it plays or seeks. */
const MAX_PAGE_BYTES = 256 * 1024;

/**
 * Serve an audio attachment with HTTP range support. Each request reads only
 * the requested byte page from Jazz (a typed large-value selection), so
 * seeking into a long recording never loads the whole file.
 */
export async function attachment(request: Request, id: string): Promise<Response> {
  try {
    // Reads as the user, so an attachment in someone else's workspace is not found.
    const db = await userDb(request);
    const meta = await db.one(app.attachments.where({ id }).select("mediaType", "byteLength"), {
      tier: "remote",
    });
    if (!meta) return Response.json({ error: "attachment not found" }, { status: 404 });

    const range = parseRange(request.headers.get("range"), meta.byteLength);
    if (range === "invalid")
      return new Response(null, {
        status: 416,
        headers: { "content-range": `bytes */${meta.byteLength}` },
      });
    const start = range?.start ?? 0;
    const end = Math.min(range?.end ?? meta.byteLength, start + MAX_PAGE_BYTES);
    const page = await db.one(
      app.attachments.where({ id }).select({ payload: { from: start, to: end } }),
    );
    const bytes = page?.payload ?? new Uint8Array();
    const partial = range !== undefined || end < meta.byteLength;
    return new Response(bytes as Uint8Array<ArrayBuffer>, {
      status: partial ? 206 : 200,
      headers: {
        "accept-ranges": "bytes",
        "cache-control": "private, max-age=3600",
        "content-length": String(bytes.byteLength),
        "content-type": meta.mediaType,
        ...(partial ? { "content-range": `bytes ${start}-${end - 1}/${meta.byteLength}` } : {}),
      },
    });
  } catch (error) {
    return errorResponse(error);
  }
}

/** Parse a single `bytes=start-end` range into a half-open [start, end). */
function parseRange(header: string | null, size: number) {
  if (!header) return undefined;
  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!match || (!match[1] && !match[2])) return "invalid" as const;
  const start = match[1] ? Number(match[1]) : Math.max(0, size - Number(match[2]));
  const end = match[1] && match[2] ? Number(match[2]) + 1 : size;
  if (start >= size || end <= start) return "invalid" as const;
  return { start, end: Math.min(end, size) };
}
