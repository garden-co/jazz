import { afterEach, expect, it } from "vitest";
import { schema as s, type Db, parseSnapshotRef, serializeSnapshotRef } from "../../src/index.js";
import { createBrowserTestDb as createDb, uniqueDbName } from "./support.js";

const app = s.defineApp(
  {
    documents: s.table({ branch: s.string(), title: s.string() }, {}).branchBy("branch"),
  },
  { migrations: "none" },
);
let db: Db | undefined;
afterEach(async () => {
  await db?.shutdown();
});

it("captures a frozen base offline and restores it after reopening persistent browser storage", async () => {
  const config = {
    appId: "browser-snapshot-capture",
    driver: { type: "persistent", dbName: uniqueDbName("snapshot-capture") },
  } as const;
  db = await createDb(config);
  const original = db.insert(
    app.documents,
    { branch: "main", title: "Original" },
    { branch: "main" },
  ).value;
  const inherited = db.insert(
    app.documents,
    { branch: "main", title: "Inherited original" },
    { branch: "main" },
  ).value;
  const capture = db.captureSnapshot();
  db.update(app.documents, original.id, { title: "Changed" }, { branch: "main" });
  db.update(app.documents, inherited.id, { title: "Inherited changed" }, { branch: "main" });
  const persisted = serializeSnapshotRef(await capture);
  const frozen = { branch: "draft", base: ["main", parseSnapshotRef(persisted)] } as const;
  expect(await db.one(app.documents.where({ id: original.id }), frozen)).toMatchObject({
    title: "Original",
  });
  expect(
    await db.one(app.documents.where({ id: original.id }), { branch: "draft", base: "main" }),
  ).toMatchObject({ title: "Changed" });
  await db.update(app.documents, original.id, { title: "Draft" }, frozen).wait({ tier: "local" });
  await db.shutdown();
  db = await createDb(config);
  const restored = { branch: "draft", base: ["main", parseSnapshotRef(persisted)] } as const;
  expect(await db.one(app.documents.where({ id: original.id }), restored)).toMatchObject({
    title: "Draft",
  });
  expect(await db.one(app.documents.where({ id: inherited.id }), restored)).toMatchObject({
    title: "Inherited original",
  });
  expect(await db.one(app.documents.where({ id: original.id }), { branch: "main" })).toMatchObject({
    title: "Changed",
  });
});
