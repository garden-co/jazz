import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import type { RowOf } from "../../src/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import type { Db } from "../../src/runtime/db.js";
import { uniqueDbName } from "./factories.js";

import { app as priorityApp } from "./apps/transformed-priority/jazz.generated.js";
import { app as relatedApp } from "./apps/transformed-related/jazz.generated.js";

type Priority = "low" | "medium" | "high";

describe("TS transformed columns", () => {
  let db: Db | undefined;

  beforeEach(async () => {
    db = await createDb({
      ...(await localAccountConfig("test-app")),
      driver: { type: "persistent", dbName: uniqueDbName("transformed-columns") },
    });
  });

  afterEach(async () => {
    await db?.shutdown();
  });

  it("transforms individual columns on reads, inserts, updates, and subscriptions", async () => {
    const activeDb = db!;

    const { value: inserted } = activeDb.insert(priorityApp.priorities, {
      label: "Upgrade docs",
      score: "high",
    });

    expectTypeOf(inserted.score).toEqualTypeOf<Priority>();
    expect(inserted.score).toBe("high");

    activeDb.update(priorityApp.priorities, inserted.id, { score: "low" });

    const byRawStoredValue = await activeDb.one(priorityApp.priorities.where({ score: 1 }));
    expect(byRawStoredValue).toMatchObject({
      id: inserted.id,
      label: "Upgrade docs",
      score: "low",
    });

    let resolveUpdate: (all: RowOf<typeof priorityApp.priorities>[]) => void = () => {};
    const nextUpdate = new Promise<RowOf<typeof priorityApp.priorities>[]>((resolve) => {
      resolveUpdate = resolve;
    });
    const unsubscribe = activeDb.subscribe(priorityApp.priorities.where({}), (rows) => {
      if (rows.some((row) => row.id === inserted.id && row.score === "medium")) {
        resolveUpdate(rows);
      }
    });

    activeDb.update(priorityApp.priorities, inserted.id, { score: "medium" });

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

    const { value: firstItem } = activeDb.insert(relatedApp.items, {
      score: 30,
      label: "alpha",
    });
    const { value: secondItem } = activeDb.insert(relatedApp.items, {
      score: 70,
      label: "beta",
    });
    const { value: emptyParent } = activeDb.insert(relatedApp.parents, {
      item: null,
      itemIds: [],
    });
    const { value: populatedParent } = activeDb.insert(relatedApp.parents, {
      item: `item:${firstItem.id}`,
      itemIds: [firstItem.id, secondItem.id],
    });

    const emptyIncluded = await activeDb.one(
      relatedApp.parents
        .where({ id: { eq: emptyParent.id } })
        .include({ itemRelation: true, items: true }),
    );
    expect(emptyIncluded?.itemRelation).toBeNull();
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
      relatedApp.parents
        .where({ id: { eq: populatedParent.id } })
        .include({ itemRelation: true, items: true }),
    );
    expect(included?.itemRelation).toEqual(firstExpected);
    expect(included?.items).toHaveLength(2);
    expect(included?.items).toEqual(expect.arrayContaining([firstExpected, secondExpected]));

    const hopped = await activeDb.one(
      relatedApp.parents.where({ id: { eq: populatedParent.id } }).hopTo("itemRelation"),
    );
    expect(hopped).toEqual(firstExpected);
  });

  it("applies partial selection before root transforms and supports legacy transform metadata", async () => {
    const activeDb = db!;
    const { value: item } = activeDb.insert(relatedApp.items, {
      score: 30,
      label: "alpha",
    });

    const partial = await activeDb.one(
      relatedApp.items.where({ id: { eq: item.id } }).select({ label: { fromUtf8: 0, toUtf8: 1 } }),
    );
    expect(partial?.label).toBe("label:a");

    const built = relatedApp.items.where({ id: { eq: item.id } });
    const legacyQuery = {
      _table: built._table,
      _schema: built._schema,
      _columnTransforms: built._columnTransforms,
      _build: () => built._build(),
      _rowType: built._rowType,
    };
    await expect(activeDb.one(legacyQuery)).resolves.toMatchObject({
      id: item.id,
      score: 30,
      label: "label:alpha",
    });
  });
});
