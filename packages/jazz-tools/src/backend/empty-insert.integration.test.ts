import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, startTestJazz, type TestJazz } from "../testing/index.js";

describe("inserting with every optional column omitted through the native backend", () => {
  it("creates the row and syncs it to another session", async () => {
    const app = s.defineApp(
      {
        notes: s.table({ label: s.string().optional(), rank: s.int().optional() }, {}),
        posters: s.table({ title: s.string().optional(), metadata: s.json().optional() }, {}),
      },
      { migrations: "none" },
    );
    const server = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });
    // Each session is an app's own Jazz (createJazz) in front of the core.
    const sessions: TestJazz[] = [];
    const openSession = async () => {
      const session = await startTestJazz({ app, permissions: {}, core: server });
      sessions.push(session);
      return session.jazz.db;
    };
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: {},
      });
      const writer = await openSession();

      const note = await writer.insert(app.notes, {}).wait({ tier: "global" });
      const poster = await writer.insert(app.posters, {}).wait({ tier: "global" });
      const inTransaction = await writer.transaction((tx) => tx.insert(app.notes, {}));
      await inTransaction.wait({ tier: "global" });

      const reader = await openSession();
      const notes = await reader.all(app.notes, { tier: "remote" });
      expect(notes).toHaveLength(2);
      expect(notes).toContainEqual({ id: note.id, label: null, rank: null });
      expect(await reader.all(app.posters, { tier: "remote" })).toEqual([
        { id: poster.id, title: null, metadata: null },
      ]);

      // The created row behaves like any other: a later update applies.
      await writer.update(app.notes, note.id, { label: "set" }).wait({ tier: "global" });
      expect(await reader.all(app.notes.where({ label: "set" }), { tier: "remote" })).toEqual([
        { id: note.id, label: "set", rank: null },
      ]);
    } finally {
      for (const session of sessions) await session.stop();
      await server.stop();
    }
  }, 30_000);
});
