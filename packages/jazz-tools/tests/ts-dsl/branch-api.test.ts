import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import type { Db } from "../../src/runtime/db.js";
import { parseSnapshotRef, serializeSnapshotRef } from "../../src/runtime/snapshot-ref.js";

const schema = {
  documents: s
    .table(
      {
        branch: s.string(),
        title: s.string(),
      },
      {},
    )
    .branchBy("branch"),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);

const referenceSchema = {
  scenarios: s.table(
    {
      name: s.string(),
      base_scenario_id: s.uuid().optional(),
    },
    {
      base_scenario: s.rel("scenarios", "base_scenario_id"),
      scenariosViaBase_scenario: s.reverse("scenarios", "base_scenario"),
      tasksViaScenario: s.reverse("tasks", "scenario"),
    },
  ),
  tasks: s
    .table(
      {
        scenario_id: s.uuid(),
        title: s.string(),
      },
      { scenario: s.rel("scenarios", "scenario_id") },
    )
    .branchBy("scenario_id"),
};

type ReferenceAppSchema = s.Schema<typeof referenceSchema>;
const referenceApp: s.App<ReferenceAppSchema> = s.defineApp(referenceSchema);

describe("branch API", () => {
  let db: Db | undefined;

  afterEach(async () => {
    await db?.shutdown();
  });

  it("uses scalar selectors for a single string branch column", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-string-shorthand")),
      driver: { type: "memory" },
    });

    const mainDocument = db.insert(
      app.documents,
      { branch: "main", title: "Main title" },
      { branch: "main" },
    ).value;

    expect(await db.all(app.documents, { branch: "draft", base: "main" })).toEqual([
      { ...mainDocument, branch: "draft" },
    ]);

    db.update(
      app.documents,
      mainDocument.id,
      { title: "Draft title" },
      { branch: "draft", base: "main" },
    );

    expect(await db.one(app.documents.where({ id: mainDocument.id }), { branch: "main" })).toEqual(
      mainDocument,
    );
    expect(
      await db.one(app.documents.where({ id: mainDocument.id }), {
        branch: "draft",
        base: "main",
      }),
    ).toMatchObject({ branch: "draft", title: "Draft title" });
  });

  it("rejects updates that try to change a branch column", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-immutable-column")),
      driver: { type: "memory" },
    });

    const document = db.insert(
      app.documents,
      { branch: "draft", title: "Draft title" },
      { branch: "draft" },
    ).value;

    await expect(
      db!
        .update(app.documents, document.id, { branch: "published" }, { branch: "draft" })
        .wait({ tier: "local" }),
    ).rejects.toThrow(
      "Schema: invalid mergeable commit: branch column does not match exact branch key",
    );

    expect(await db.one(app.documents.where({ id: document.id }), { branch: "draft" })).toEqual(
      document,
    );
    expect(
      await db.one(app.documents.where({ id: document.id }), { branch: "published" }),
    ).toBeNull();
  });

  it("captures pending local writes, restores the reference, and freezes the base below a live draft", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-frozen-capture")),
      driver: { type: "memory" },
    });
    const original = db.insert(
      app.documents,
      { branch: "main", title: "Original" },
      { branch: "main" },
    ).value;
    const capture = db.captureSnapshot();
    db.update(app.documents, original.id, { title: "Changed" }, { branch: "main" });
    const snapshot = parseSnapshotRef(
      JSON.parse(JSON.stringify(serializeSnapshotRef(await capture))),
    );
    const frozen = { branch: "draft", base: ["main", snapshot] } as const;
    expect(await db.one(app.documents, frozen)).toMatchObject({
      id: original.id,
      branch: "draft",
      title: "Original",
    });
    expect(await db.one(app.documents, { branch: "draft", base: "main" })).toMatchObject({
      title: "Changed",
    });
    db.update(app.documents, original.id, { title: "Draft" }, frozen);
    expect(await db.one(app.documents, frozen)).toMatchObject({ title: "Draft" });
    expect(await db.one(app.documents, { branch: "main" })).toMatchObject({ title: "Changed" });
  });

  it("does not capture uncommitted transaction writes", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-capture-staged")),
      driver: { type: "memory" },
    });
    const original = db.insert(
      app.documents,
      { branch: "main", title: "Original" },
      { branch: "main" },
    ).value;
    const tx = db.beginTransaction();
    tx.update(app.documents, original.id, { title: "Uncommitted" }, { branch: "main" });
    const snapshot = await db.captureSnapshot();
    tx.commit();
    expect(
      await db.one(app.documents, { branch: "draft", base: ["main", snapshot] }),
    ).toMatchObject({ title: "Original" });
    expect(await db.one(app.documents, { branch: "main" })).toMatchObject({ title: "Uncommitted" });
  });

  it("reports unavailable history when restoring onto an empty store", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-capture-origin")),
      driver: { type: "memory" },
    });
    db.insert(app.documents, { branch: "main", title: "Original" }, { branch: "main" });
    const restored = parseSnapshotRef(serializeSnapshotRef(await db.captureSnapshot()));
    const empty = await createDb({
      ...(await localAccountConfig("branch-api-capture-empty")),
      driver: { type: "memory" },
    });
    try {
      await expect(
        empty.all(app.documents, { branch: "draft", base: ["main", restored] }),
      ).rejects.toMatchObject({
        code: "not_observed",
        message: expect.stringContaining("Snapshot history is unavailable"),
      });
    } finally {
      await empty.shutdown();
    }
  });

  it("reports capture before schema attachment and after shutdown", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-capture-lifecycle")),
      driver: { type: "memory" },
    });
    await expect(db.captureSnapshot()).rejects.toThrow("Run a query or mutation first");
    await db.shutdown();
    await expect(db.captureSnapshot()).rejects.toThrow();
  });

  it("uses referenced row IDs as scalar branch selectors", async () => {
    db = await createDb({
      ...(await localAccountConfig("branch-api-reference-shorthand")),
      driver: { type: "memory" },
    });

    const main = db.insert(referenceApp.scenarios, { name: "Main" }).value;
    const draft = db.insert(referenceApp.scenarios, {
      name: "Draft",
      base_scenario_id: main.id,
    }).value;
    const task = db.insert(
      referenceApp.tasks,
      { scenario_id: main.id, title: "Inherited task" },
      { branch: main.id },
    ).value;

    expect(
      await db.one(referenceApp.tasks.where({ id: task.id }), {
        branch: draft.id,
        base: main.id,
      }),
    ).toEqual({ ...task, scenario_id: draft.id });
  });
});
