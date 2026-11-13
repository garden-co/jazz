import { createTestJazz, remote, type Jazz } from "../../src/backend/create-jazz.js";
import {
  liveAuthorityApp as app,
  liveAuthorityPermissions,
  type LiveAuthoritySeed,
} from "./live-authority-replay-schema.js";
import type { JazzServerInfo } from "./testing-server.js";

// The app's backend, run against the test core in `info` (as the app runs it,
// but on `{ migrations: "none" }`, as `startTestJazz` runs a test's app).
const backends = new Map<string, Jazz>();
export async function liveAuthorityBackendOpen(info: JazzServerInfo): Promise<LiveAuthoritySeed> {
  const jazz = createTestJazz({
    app,
    permissions: liveAuthorityPermissions,
    store: remote({ url: info.serverUrl, appId: info.appId, secret: info.adminSecret }),
  });
  backends.set(info.appId, jazz);
  try {
    const db = jazz.db;
    const parent = await db.insert(app.parents, { name: "Parent" }).wait({ tier: "global" });
    const author = await db.insert(app.authors, { name: "Author" }).wait({ tier: "global" });
    const label = await db.insert(app.labels, { name: "Label" }).wait({ tier: "global" });
    await db.insert(app.unrelated, { value: "still usable" }).wait({ tier: "global" });
    const seed = { parentId: parent.id, authorId: author.id, labelId: label.id, itemId: "" };
    seed.itemId = await liveAuthorityBackendInsert(info.appId, seed, "hydrated");
    return seed;
  } catch (error) {
    try {
      await liveAuthorityBackendClose(info.appId);
    } catch (cleanupError) {
      throw new AggregateError([error, cleanupError], "Backend seed and cleanup failed");
    }
    throw error;
  }
}
export async function liveAuthorityBackendInsert(
  appId: string,
  seed: LiveAuthoritySeed,
  title: string,
): Promise<string> {
  const jazz = backends.get(appId);
  if (!jazz) throw new Error("Live-authority backend was not opened");
  const db = jazz.db;
  const row = await db
    .insert(app.items, {
      title,
      parent_id: seed.parentId,
      author_id: seed.authorId,
      label_id: seed.labelId,
    })
    .wait({ tier: "global" });
  const readable = await db.all(app.items.where({ id: row.id }), { tier: "remote" });
  if (readable.length !== 1) throw new Error("Globally settled backend insert is not readable");
  return row.id;
}
export async function liveAuthorityBackendClose(appId: string): Promise<void> {
  const jazz = backends.get(appId);
  await jazz?.close();
  if (backends.get(appId) === jazz) backends.delete(appId);
}
