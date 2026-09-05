import { createJazzContext } from "../../src/backend/create-jazz-context.js";
import {
  liveEdgeApp as app,
  liveEdgePermissions,
  type LiveEdgeSeed,
} from "./live-edge-replay-schema.js";
import type { JazzServerInfo } from "./testing-server.js";

const contexts = new Map<string, ReturnType<typeof createJazzContext>>();
export async function liveEdgeBackendOpen(info: JazzServerInfo): Promise<LiveEdgeSeed> {
  const context = createJazzContext({
    ...info,
    backendSecret: "jazz-browser-test-backend",
    app,
    permissions: liveEdgePermissions,
    driver: { type: "memory" },
    tier: "edge",
    defaultDurabilityTier: "global",
  });
  contexts.set(info.appId, context);
  const db = context.asBackend();
  const parent = await db.insert(app.parents, { name: "Parent" }).wait({ tier: "global" });
  const author = await db.insert(app.authors, { name: "Author" }).wait({ tier: "global" });
  const label = await db.insert(app.labels, { name: "Label" }).wait({ tier: "global" });
  await db.insert(app.unrelated, { value: "still usable" }).wait({ tier: "global" });
  const seed = { parentId: parent.id, authorId: author.id, labelId: label.id, itemId: "" };
  seed.itemId = await liveEdgeBackendInsert(info.appId, seed, "hydrated");
  return seed;
}
export async function liveEdgeBackendInsert(
  appId: string,
  seed: LiveEdgeSeed,
  title: string,
): Promise<string> {
  const context = contexts.get(appId);
  if (!context) throw new Error("Live-edge backend was not opened");
  const db = context.asBackend();
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
  const context = contexts.get(appId);
  contexts.delete(appId);
  await context?.shutdown();
}
