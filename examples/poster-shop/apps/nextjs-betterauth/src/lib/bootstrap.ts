import { randomUUID } from "node:crypto";
import type { Db } from "jazz-tools";
import { app } from "@/schema";
import { demoPoster } from "@/src/lib/demo-poster";
import { authorForSession } from "@/src/lib/identity";
import { withBoundedConflictRetry } from "@/src/lib/retry";

export const POSTER_WIDTH = 1080;
export const POSTER_HEIGHT = 1350;

// Concurrent first opens for one account in this process share one run; the
// exclusive transaction below covers races across processes.
const inFlight = new Map<string, Promise<string>>();

/**
 * The only first-open side effect, and the only place a canvas is created. It
 * runs server-side on a backend-authority `db` (the routes pass
 * `client.withAttributionForRequest(request)`), never from a query hook or a
 * browser-held secret. The exclusive transaction makes it idempotent: a user
 * who already has a membership gets nothing new, and two racing first opens
 * cannot both seed a poster. Resolves to the user's first canvas id.
 */
export function ensurePersonalCanvas(
  db: Db,
  accountId: string,
  displayName: string,
): Promise<string> {
  let pending = inFlight.get(accountId);
  if (!pending) {
    pending = seedPersonalCanvas(db, accountId, displayName).finally(() =>
      inFlight.delete(accountId),
    );
    inFlight.set(accountId, pending);
  }
  return pending;
}

/** The cross-process guarantee on its own, without the per-process dedupe. */
export async function seedPersonalCanvas(db: Db, accountId: string, displayName: string) {
  const memberAuthor = authorForSession(accountId);
  return await withBoundedConflictRetry(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const existing = await tx.one(app.canvasMembers.where({ memberAuthor }));
      if (existing) return existing.canvasId;
      const canvasId = randomUUID();
      const poster = demoPoster(displayName, randomUUID);
      tx.insert(
        app.canvases,
        { title: poster.title, width: POSTER_WIDTH, height: POSTER_HEIGHT },
        { id: canvasId },
      );
      tx.insert(app.canvasMembers, { canvasId, memberAuthor, role: "admin" }, { id: randomUUID() });
      for (const { id, ...layer } of poster.layers) {
        tx.insert(app.layers, { canvasId, ...layer }, { id });
      }
      for (const { id, assetId: _assetId, text, ...shape } of poster.shapes) {
        tx.insert(app.shapes, { canvasId, ...shape, ...(text ? { text } : {}) }, { id });
      }
      tx.insert(
        app.checkpoints,
        { canvasId, label: poster.checkpoint.label, snapshot: poster.checkpoint.snapshot },
        { id: randomUUID() },
      );
      return canvasId;
    });
    await write.wait();
    return write.value;
  });
}
