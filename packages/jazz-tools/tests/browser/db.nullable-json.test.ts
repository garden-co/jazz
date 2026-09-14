import { expect, it } from "vitest";
import { schema as s, generateAuthSecret, type JsonValue } from "../../src/index.js";
import { createBrowserTestDb } from "./support.js";
import { deploy } from "../../src/dev/catalogue.js";
import { getJazzServerInfo, stopJazzServer } from "./testing-server.js";

it("preserves nullable JSON across persistent browser reopen and subsequent writes", async () => {
  const app = s.defineApp({
    documents: s.table({ label: s.string(), payload: s.json().optional() }),
  });
  const permissions = s.definePermissions(app, ({ policy }) => [
    policy.documents.allowRead.always(),
    policy.documents.allowInsert.always(),
    policy.documents.allowUpdate.always(),
  ]);
  const server = await getJazzServerInfo(crypto.randomUUID());
  await deploy({ ...server, schema: app, permissions });
  const config = {
    appId: server.appId,
    serverUrl: server.serverUrl,
    secret: generateAuthSecret(),
    driver: { type: "persistent" as const, dbName: `nullable-json-${crypto.randomUUID()}` },
  };
  let db = await createBrowserTestDb(config);
  try {
    const values: JsonValue[] = [
      null,
      { nested: null },
      [null, 1],
      "null",
      { body: "x".repeat(100_000) },
    ];
    const ids = [];
    for (const payload of values) {
      ids.push(
        (await db.insert(app.documents, { label: "document", payload }).wait({ tier: "global" }))
          .id,
      );
    }
    const streamed = await db.insertStreaming(app.documents, {
      label: "streamed root null",
      payload: (async function* () {
        yield " ".repeat(4095);
        yield "null";
        yield "\n".repeat(90_000);
      })(),
    });
    await streamed.wait({ tier: "global" });
    ids.push(streamed.value.id);
    values.push(null);
    await db.shutdown();
    db = await createBrowserTestDb(config);
    for (let index = 0; index < ids.length; index++) {
      const id = ids[index]!;
      expect(await db.one(app.documents.where({ id }), { tier: "local" })).toMatchObject({
        payload: values[index],
      });
      await db.update(app.documents, id, { label: "patched" }).wait({ tier: "global" });
      expect(await db.one(app.documents.where({ id }), { tier: "local" })).toMatchObject({
        payload: values[index],
      });
      await db.update(app.documents, id, { payload: null }).wait({ tier: "global" });
    }
    expect(await db.all(app.documents.where({ payload: null }), { tier: "local" })).toHaveLength(
      values.length,
    );
  } finally {
    await db.shutdown();
    await stopJazzServer(server.serverUrl);
  }
}, 90_000);
