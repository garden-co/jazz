import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { schema as s, schemaToWasm, TypedTableQueryBuilder } from "../../src/index.js";
import { schemaDefinitionToAst } from "../../src/migrations.js";
import type { CompiledPermissions } from "../../src/permissions/index.js";
import { createDb, type Db } from "../../src/runtime/db.js";
import { mergePermissionsIntoSchema } from "../../src/schema-permissions.js";
import { uniqueDbName } from "./factories";

type Priority = "low" | "medium" | "high";

const prioritySchema = {
  priorities: s.table({
    label: s.string(),
    score: s.int().transform<Priority>({
      from: (score) => (score >= 8 ? "high" : score >= 4 ? "medium" : "low"),
      to: (priority) => ({ low: 1, medium: 5, high: 10 })[priority],
    }),
  }),
};

type PriorityAppSchema = s.Schema<typeof prioritySchema>;
const priorityApp: s.App<PriorityAppSchema> = s.defineApp(prioritySchema);
const priorityPermissions = s.definePermissions(priorityApp, ({ policy }) => {
  policy.priorities.allowRead.where({});
  policy.priorities.allowInsert.where({});
  policy.priorities.allowUpdate.where({});
});
const priorityAppWithPermissions: s.App<PriorityAppSchema> = applyPermissions(priorityPermissions);

function applyPermissions(permissions: CompiledPermissions): s.App<PriorityAppSchema> {
  const wasmSchema = schemaToWasm(
    mergePermissionsIntoSchema(schemaDefinitionToAst(prioritySchema), permissions),
  );

  return {
    priorities: new TypedTableQueryBuilder(
      "priorities",
      wasmSchema,
      priorityApp.priorities._columnTransforms,
    ),
    wasmSchema,
  } as s.App<PriorityAppSchema>;
}
const relatedSchema = {
  parents: s.table({
    itemId: s.ref("items").optional(),
    itemIds: s.array(s.ref("items")),
  }),
  items: s.table({
    score: s.int().transform<number>({
      from: (value) => value * 10,
      to: (value) => value / 10,
    }),
    label: s.string().transform<string>({
      from: (value) => `label:${value}`,
      to: (value) => value.replace(/^label:/, ""),
    }),
  }),
};

type RelatedAppSchema = s.Schema<typeof relatedSchema>;
const relatedApp: s.App<RelatedAppSchema> = s.defineApp(relatedSchema);
const relatedPermissions = s.definePermissions(relatedApp, ({ policy }) => {
  policy.parents.allowRead.where({});
  policy.parents.allowInsert.where({});
  policy.items.allowRead.where({});
  policy.items.allowInsert.where({});
});
const relatedAppWithPermissions: s.App<RelatedAppSchema> = applyRelatedPermissions(relatedPermissions);

function applyRelatedPermissions(permissions: CompiledPermissions): s.App<RelatedAppSchema> {
  const wasmSchema = schemaToWasm(
    mergePermissionsIntoSchema(schemaDefinitionToAst(relatedSchema), permissions),
  );

  return {
    parents: new TypedTableQueryBuilder(
      "parents",
      wasmSchema,
      relatedApp.parents._columnTransforms,
    ),
    items: new TypedTableQueryBuilder("items", wasmSchema, relatedApp.items._columnTransforms),
    wasmSchema,
  } as s.App<RelatedAppSchema>;
}


describe("TS transformed columns", () => {
  let db: Db | undefined;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName: uniqueDbName("transformed-columns") },
    });
  });

  afterEach(async () => {
    await db?.shutdown();
  });

  it("transforms individual columns on reads, inserts, updates, and subscriptions", async () => {
    const activeDb = db!;

    const { value: inserted } = activeDb.insert(priorityAppWithPermissions.priorities, {
      label: "Upgrade docs",
      score: "high",
    });

    expectTypeOf(inserted.score).toEqualTypeOf<Priority>();
    expect(inserted.score).toBe("high");

    activeDb.update(priorityAppWithPermissions.priorities, inserted.id, { score: "low" });

    const byRawStoredValue = await activeDb.one(
      priorityAppWithPermissions.priorities.where({ score: 1 }),
    );
    expect(byRawStoredValue).toMatchObject({
      id: inserted.id,
      label: "Upgrade docs",
      score: "low",
    });

    let resolveUpdate: (all: s.RowOf<typeof priorityApp.priorities>[]) => void = () => {};
    const nextUpdate = new Promise<s.RowOf<typeof priorityApp.priorities>[]>((resolve) => {
      resolveUpdate = resolve;
    });
    const unsubscribe = activeDb.subscribe(
      priorityAppWithPermissions.priorities.where({}),
      (rows) => {
        if (rows.some((row) => row.id === inserted.id && row.score === "medium")) {
          resolveUpdate(rows);
        }
      },
    );

    activeDb.update(priorityAppWithPermissions.priorities, inserted.id, { score: "medium" });

    await expect(nextUpdate).resolves.toContainEqual(
      expect.objectContaining({
        id: inserted.id,
        score: "medium",
      }),
    );

    unsubscribe();
  });
  it("applies target transforms in included and hopped rows without changing relation shape", async () => {
    const activeDb = db!;

    const { value: firstItem } = activeDb.insert(relatedAppWithPermissions.items, {
      score: 3,
      label: "alpha",
    });
    const { value: secondItem } = activeDb.insert(relatedAppWithPermissions.items, {
      score: 7,
      label: "beta",
    });
    const { value: emptyParent } = activeDb.insert(relatedAppWithPermissions.parents, {
      itemId: null,
      itemIds: [],
    });
    const { value: populatedParent } = activeDb.insert(relatedAppWithPermissions.parents, {
      itemId: firstItem.id,
      itemIds: [firstItem.id, secondItem.id],
    });

    const emptyIncluded = await activeDb.one(
      relatedAppWithPermissions.parents
        .where({ id: { eq: emptyParent.id } })
        .include({ item: true, items: true }),
    );
    expect(emptyIncluded?.item).toBeNull();
    expect(emptyIncluded?.items).toEqual([]);

    const firstExpected = {
      id: firstItem.id,
      score: 30,
      label: "label:alpha",
    };
    const secondExpected = {
      id: secondItem.id,
      score: 70,
      label: "label:beta",
    };
    const included = await activeDb.one(
      relatedAppWithPermissions.parents
        .where({ id: { eq: populatedParent.id } })
        .include({ item: true, items: true }),
    );
    expect(included?.item).toEqual(firstExpected);
    expect(included?.items).toHaveLength(2);
    expect(included?.items).toEqual(expect.arrayContaining([firstExpected, secondExpected]));

    const hopped = await activeDb.one(
      relatedAppWithPermissions.parents
        .where({ id: { eq: populatedParent.id } })
        .hopTo("item"),
    );
    expect(hopped).toEqual(firstExpected);
  });
});
