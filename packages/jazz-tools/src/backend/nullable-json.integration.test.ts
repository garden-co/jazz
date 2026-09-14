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
      // String inputs follow the existing raw-JSON-source API contract.
      const authored = typeof payload === "string" ? JSON.stringify(payload) : payload;
      await db.update(app.documents, row.id, { payload: authored }).wait({ tier: "global" });
      await db.update(app.documents, row.id, { label: "patched" }).wait({ tier: "global" });
      expect(await db.one(app.documents.where({ id: row.id }), { tier: "edge" })).toMatchObject({
        payload,
      });
    }
    // Unquoted raw source is root null; quoted source above remains a string.
    await db.update(app.documents, row.id, { payload: "null" }).wait({ tier: "global" });
    expect(await db.one(app.documents.where({ id: row.id }), { tier: "edge" })).toMatchObject({
      payload: null,
    });
    const streamed = await db.insertStreaming(app.documents, {
      label: "streamed root null",
      payload: (async function* () {
        yield " ".repeat(4095);
        yield "null";
        yield "\n".repeat(90_000);
      })(),
    });
    await streamed.wait({ tier: "global" });
    expect(
      await db.one(app.documents.where({ id: streamed.value.id }), { tier: "edge" }),
    ).toMatchObject({ payload: null });
    const nulls = await db.all(app.documents.where({ payload: null }), { tier: "edge" });
    expect(nulls.map((value) => value.id).sort()).toEqual([row.id, streamed.value.id].sort());
  } finally {
    await session?.close();
    await server.stop();
  }
}, 60_000);
