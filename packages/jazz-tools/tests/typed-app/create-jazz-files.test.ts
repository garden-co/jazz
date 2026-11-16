import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { startLocalJazzServer, startTestJazz, testApp } from "../../src/testing/index.js";

const app = testApp({
  tables: {
    notes: {
      columns: { label: "string", ownerId: "uuid?" },
    },
  },
});

describe("createJazz with the files", () => {
  it("publishes the bound app with jazz.permissions.json and serves writes it allows", async () => {
    const core = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });
    const jazz = await startTestJazz({
      app,
      permissions: { format: 1, tables: { notes: { read: "true", insert: "true" } } },
      core,
    });
    try {
      const db = jazz.jazz.db;
      const note = await db.insert(app.notes, { label: "hello" }).wait({ tier: "global" });
      expect(await db.all(app.notes, { tier: "remote" })).toEqual([
        { id: note.id, label: "hello", ownerId: null },
      ]);
    } finally {
      await jazz.stop();
      await core.stop();
    }
  }, 30_000);

  it("refuses a permissions file the loader refuses, naming it", async () => {
    const core = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });
    try {
      await expect(
        startTestJazz({
          app,
          permissions: { format: 1, tables: { nope: { read: "true" } } },
          core,
        }),
      ).rejects.toThrow(/createJazz: .*nope/);
      await expect(
        startTestJazz({ app, permissions: { notes: {} } as never, core }),
      ).rejects.toThrow(/must be the app's jazz\.permissions\.json/);
    } finally {
      await core.stop();
    }
  }, 30_000);

  it("refuses an app that isn't bound", async () => {
    const core = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });
    try {
      await expect(
        startTestJazz({
          app: { notes: { columns: [] } } as never,
          permissions: { format: 1, tables: {} },
          core,
        }),
      ).rejects.toThrow(/jazz\.generated\.ts exports/);
    } finally {
      await core.stop();
    }
  }, 30_000);
});
