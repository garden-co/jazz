import { app } from "@/schema";
import { errorResponse, userDb } from "@/src/server/access";

export const runtime = "nodejs";

/** Largest page served per request; the audio element asks for the rest as it plays or seeks. */
const MAX_PAGE_BYTES = 256 * 1024;

/**
 * Serve an audio attachment with HTTP range support. Each request reads only
 * the requested byte page from Jazz (a typed large-value selection), so
 * seeking into a long recording never loads the whole file.
 */
export async function GET(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    // Reads as the user, so an attachment in someone else's workspace is not found.
    const db = await userDb(request);
    const id = (await params).id;
    const meta = await db.one(app.attachments.where({ id }).select("mediaType", "byteLength"), {
      tier: "global",
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
