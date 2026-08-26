import { randomUUID } from "node:crypto";
import { app } from "@/schema";
import { authJazzContext } from "@/src/lib/auth-jazz-context";

// The only first-open side effect. It executes server-side with backend
// authority, never from a query hook or a browser-held secret.
export async function ensurePersonalCanvas(userId: string, displayName: string) {
  const db = authJazzContext().asBackend(app);
  for (;;) {
    try {
      const write = await db.exclusiveTransaction(async (tx) => {
        const memberships = await tx.all(app.canvasMembers.where({ userId }));
        if (memberships[0]) return memberships[0].canvasId;
        const canvasId = randomUUID();
        tx.insert(
          app.canvases,
          { title: `${displayName}'s poster`, width: 1080, height: 1350 },
          { id: canvasId },
        );
        tx.insert(app.canvasMembers, { canvasId, userId, role: "admin" }, { id: randomUUID() });
        tx.insert(
          app.layers,
          { canvasId, name: "Artwork", zIndex: 0, visible: true },
          { id: randomUUID() },
        );
        return canvasId;
      });
      await write.wait();
      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!/exclusive_conflict|transaction_conflict|cascade_rejected/.test(message)) throw error;
    }
  }
}
