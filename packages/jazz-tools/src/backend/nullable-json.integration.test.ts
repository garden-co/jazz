import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { schema as s, type JsonValue } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "./index.js";

// Retains the public nullable-JSON regression intent from Hussein's #2736.
it("round-trips nullable JSON and preserves omitted patches through the native backend", async () => {
  const app = s.defineApp({
    documents: s.table({ label: s.string(), payload: s.json().optional() }),
  });
  const server = await startLocalJazzServer({ appId: randomUUID(), inMemory: false });
  let session: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions: {},
    });
    session = await createJazzSession({
      appId: server.appId,
      serverUrl: server.url,
      app,
      permissions: {},
      driver: { type: "memory" },
      initial: { backendSecret: server.backendSecret },
    });
    const db = session.getSnapshot().client!.db;
    const row = await db
      .insert(app.documents, { label: "document", payload: null })
      .wait({ tier: "global" });
    const values: JsonValue[] = [
      { nested: null },
      [null, 1],
      "null",
      { body: "x".repeat(100_000) },
      null,
    ];
    for (const payload of values) {
      await db.update(app.documents, row.id, { payload }).wait({ tier: "global" });
      await db.update(app.documents, row.id, { label: "patched" }).wait({ tier: "global" });
      expect(await db.one(app.documents.where({ id: row.id }), { tier: "edge" })).toMatchObject({
        payload,
      });
    }
    expect(await db.all(app.documents.where({ payload: null }), { tier: "edge" })).toMatchObject([
      { id: row.id, payload: null },
    ]);
  } finally {
    await session?.close();
    await server.stop();
  }
}, 60_000);
