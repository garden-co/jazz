import { createJazzSession } from "../../src/backend/create-jazz-session.js";
import {
  liveEdgeApp as app,
  liveEdgePermissions,
  type LiveEdgeSeed,
} from "./live-edge-replay-schema.js";
import type { JazzServerInfo } from "./testing-server.js";

const sessions = new Map<string, Awaited<ReturnType<typeof createJazzSession>>>();
export async function liveEdgeBackendOpen(info: JazzServerInfo): Promise<LiveEdgeSeed> {
  const session = await createJazzSession({
    appId: info.appId,
    serverUrl: info.serverUrl,
    initial: { backendSecret: "jazz-browser-test-backend" },
    app,
    permissions: liveEdgePermissions,
    driver: { type: "memory" },
    tier: "edge",
    defaultDurabilityTier: "global",
  });
  sessions.set(info.appId, session);
  try {
    const snapshot = session.getSnapshot();
    if (snapshot.status !== "ready" || !snapshot.client) {
      throw snapshot.error ?? new Error("Backend session is not ready");
    }
    const db = snapshot.client.db;
    const parent = await db.insert(app.parents, { name: "Parent" }).wait({ tier: "global" });
    const author = await db.insert(app.authors, { name: "Author" }).wait({ tier: "global" });
    const label = await db.insert(app.labels, { name: "Label" }).wait({ tier: "global" });
    await db.insert(app.unrelated, { value: "still usable" }).wait({ tier: "global" });
    const seed = { parentId: parent.id, authorId: author.id, labelId: label.id, itemId: "" };
    seed.itemId = await liveEdgeBackendInsert(info.appId, seed, "hydrated");
    return seed;
  } catch (error) {
    try {
      await liveEdgeBackendClose(info.appId);
    } catch (cleanupError) {
      throw new AggregateError([error, cleanupError], "Backend seed and cleanup failed");
    }
    throw error;
  }
}
export async function liveEdgeBackendInsert(
  appId: string,
  seed: LiveEdgeSeed,
  title: string,
): Promise<string> {
  const session = sessions.get(appId);
  if (!session) throw new Error("Live-edge backend was not opened");
  const snapshot = session.getSnapshot();
  if (snapshot.status !== "ready" || !snapshot.client) {
    throw snapshot.error ?? new Error("Backend session is not ready");
  }
  const db = snapshot.client.db;
  const row = await db
    .insert(app.items, {
      title,
      parent_id: seed.parentId,
      author_id: seed.authorId,
      label_id: seed.labelId,
    })
    .wait({ tier: "global" });
  const readable = await db.all(app.items.where({ id: row.id }), { tier: "global" });
  if (readable.length !== 1) throw new Error("Globally settled backend insert is not readable");
  return row.id;
}
export async function liveEdgeBackendClose(appId: string): Promise<void> {
  const session = sessions.get(appId);
  await session?.close();
  if (sessions.get(appId) === session) sessions.delete(appId);
}
