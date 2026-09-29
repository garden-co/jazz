import { randomUUID } from "node:crypto";
import { app } from "@/schema";
import { authJazzClient } from "@/src/lib/auth-jazz-client";
import { demoPoster } from "@/src/lib/demo-poster";
import { authorForSession } from "@/src/lib/identity";
import { withBoundedConflictRetry } from "@/src/lib/retry";

// The only first-open side effect. It executes server-side with backend
// authority, never from a query hook or a browser-held secret. The exclusive
// transaction makes it idempotent: a user who already has a membership gets
// nothing new, and two racing first opens cannot both seed a poster.
export async function ensurePersonalCanvas(accountId: string, displayName: string) {
  const memberAuthor = authorForSession(accountId);
  const db = (await authJazzClient()).db;
  await withBoundedConflictRetry(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const memberships = await tx.all(app.canvasMembers.where({ memberAuthor }));
      if (memberships[0]) return memberships[0].canvasId;
      const canvasId = randomUUID();
      const poster = demoPoster(displayName, randomUUID);
      tx.insert(app.canvases, { title: poster.title, width: 1080, height: 1350 }, { id: canvasId });
      tx.insert(app.canvasMembers, { canvasId, memberAuthor, role: "admin" }, { id: randomUUID() });
      for (const { id, ...layer } of poster.layers) {
        tx.insert(app.layers, { canvasId, ...layer }, { id });
      }
      for (const { id, assetId: _assetId, text, ...shape } of poster.shapes) {
        tx.insert(app.shapes, { canvasId, ...shape, ...(text ? { text } : {}) }, { id });
      }
      tx.insert(
        app.checkpoints,
        {
          canvasId,
          label: poster.checkpoint.label,
          branch: "main",
          snapshot: poster.checkpoint.snapshot,
        },
        { id: randomUUID() },
      );
      return canvasId;
    });
    await write.wait();
  });
}
