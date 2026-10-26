import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

// A remote read uses the server's current query scope and waits while offline
// (INV-API-32): only the authority confirms a remote result. The same rule is
// pinned for tabs over a browser worker and for React Native foregrounds over
// the native relay.
it("waits for the server while offline and answers once it reconnects", async () => {
  const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
  const permissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let db: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const settled = await db.insert(app.notes, { title: "settled" }).wait({ tier: "global" });
    expect(await db.all(app.notes, { tier: "remote" })).toEqual([settled]);

    await db.disconnect();
    const offline = await db.insert(app.notes, { title: "offline" }).wait({ tier: "local" });
    let answered = false;
    const remote = db.all(app.notes, { tier: "remote" }).then((rows) => {
      answered = true;
      return rows;
    });
    // Local-first reads stay usable meanwhile.
    expect(await db.all(app.notes, { tier: "local-first" })).toEqual(
      expect.arrayContaining([settled, offline]),
    );
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(answered).toBe(false);

    await db.reconnect();
    // Answered after the offline write was uploaded (read-your-writes).
    expect(await remote).toEqual(expect.arrayContaining([settled, offline]));
  } finally {
    await db?.shutdown();
    await server.stop();
  }
}, 60_000);
