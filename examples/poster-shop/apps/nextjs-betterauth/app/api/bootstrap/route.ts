import { ensurePersonalCanvas } from "@/src/lib/bootstrap";
import { authJazzClient } from "@/src/lib/auth-jazz-client";
import { verifiedRequest } from "@/src/lib/request-account";
import { BootstrapConflictError } from "@/src/lib/retry";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const caller = await verifiedRequest(await authJazzClient(), request);
  if (!caller) return Response.json({ error: "account required" }, { status: 401 });
  // Better Auth's JWT plugin signs the session user as its default payload
  // (src/lib/auth.ts sets no custom definePayload), so `name` is the display
  // name the user signed up with.
  const name = typeof caller.claims.name === "string" ? caller.claims.name : "";
  try {
    const canvasId = await ensurePersonalCanvas(caller.db, caller.accountId, name);
    return Response.json({ ok: true, canvasId });
  } catch (error) {
    // Persistent first-open conflicts are recoverable: the client retries.
    if (error instanceof BootstrapConflictError)
      return Response.json({ error: "bootstrap busy, retry" }, { status: 503 });
    throw error;
  }
}
