import { describe, expect, it } from "vitest";
import { schema } from "../../src/index.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({
  groups: schema.table({ name: schema.string() }),
  tasks: schema.table({ title: schema.string(), group_id: schema.ref("groups") }),
  notes: schema.table({ body: schema.string(), task_id: schema.ref("tasks") }),
});
const query = app.groups
  .include({
    tasksViaGroup: app.tasks.select("title").include({ notesViaTask: true }),
  })
  .requireIncludes();

// Browser donor: db.include-subscriptions.server.test.ts selected nested
// includes. This fixture replaces browser transport with the real native owner.
describe("React Native structured reads", () => {
  it("hydrates selected nested includes and isolates transaction overlays", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const group = db.insert(app.groups, { name: "group" }).value;
      const task = db.insert(app.tasks, { title: "task", group_id: group.id }).value;
      const note = db.insert(app.notes, { body: "base note", task_id: task.id }).value;
      expect(await db.all(query)).toMatchObject([
        {
          ...group,
          tasksViaGroup: [
            {
              id: task.id,
              title: "task",
              notesViaTask: [note],
            },
          ],
        },
      ]);
      const tx = db.beginExclusiveTransaction();
      tx.update(app.notes, note.id, { body: "draft note" });
      expect(await tx.all(query)).toMatchObject([
        { tasksViaGroup: [{ notesViaTask: [{ body: "draft note" }] }] },
      ]);
      expect(await db.all(query)).toMatchObject([
        { tasksViaGroup: [{ notesViaTask: [{ body: "base note" }] }] },
      ]);
      await tx.rollback();
      expect(await db.all(query)).toMatchObject([
        { tasksViaGroup: [{ notesViaTask: [{ body: "base note" }] }] },
      ]);
    });
  });

  it("delivers nested child updates and removals through shared subscription decoding", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const group = db.insert(app.groups, { name: "group" }).value;
      const task = db.insert(app.tasks, { title: "task", group_id: group.id }).value;
      const note = db.insert(app.notes, { body: "base", task_id: task.id }).value;
      const snapshots: unknown[] = [];
      const unsubscribe = db.subscribe(query, (rows) => snapshots.push(rows));
      try {
        await expect
          .poll(() => snapshots.at(-1))
          .toMatchObject([{ tasksViaGroup: [{ notesViaTask: [{ body: "base" }] }] }]);
        db.update(app.notes, note.id, { body: "updated" });
        await expect
          .poll(() => snapshots.at(-1))
          .toMatchObject([{ tasksViaGroup: [{ notesViaTask: [{ body: "updated" }] }] }]);
        db.delete(app.notes, note.id);
        await expect
          .poll(() => snapshots.at(-1))
          .toMatchObject([{ tasksViaGroup: [{ notesViaTask: [] }] }]);
      } finally {
        unsubscribe();
      }
    });
  });
});
