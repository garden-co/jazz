import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createDb, type Db } from "../../src/runtime/db.js";
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

describe("TS transformed columns", () => {
  let db: Db | undefined;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName: uniqueDbName("transformed-columns") },
      schema: priorityApp,
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

    let resolveUpdate: (all: s.RowOf<typeof priorityApp.priorities>[]) => void = () => {};
    const nextUpdate = new Promise<s.RowOf<typeof priorityApp.priorities>[]>((resolve) => {
      resolveUpdate = resolve;
    });

    const unsubscribe = activeDb.subscribeAll(priorityApp.priorities.where({}), ({ all }) => {
      if (all.some((row) => row.id === inserted.id && row.score === "medium")) {
        resolveUpdate(all);
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
});
