import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createDb, type Db } from "../../src/runtime/db.js";

const schema = {
  documents: s
    .table({
      branch: s.string(),
      title: s.string(),
    })
    .branchBy("branch"),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);

const referenceSchema = {
  scenarios: s.table({
    name: s.string(),
    base_scenario_id: s.ref("scenarios").optional(),
  }),
  tasks: s
    .table({
      scenario_id: s.ref("scenarios"),
      title: s.string(),
    })
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
      appId: "branch-api-string-shorthand",
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
      appId: "branch-api-immutable-column",
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

  it("uses referenced row IDs as scalar branch selectors", async () => {
    db = await createDb({
      appId: "branch-api-reference-shorthand",
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
