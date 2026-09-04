import { expect, it } from "vitest";
import { schema } from "../../src/schema-namespace.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});

it("runs public CRUD, query, subscription and foreground propagation through the real RN owner, then closes", async () => {
  await withNativeRelayFixture(app, async (fixture) => {
    const writer = await fixture.createDb();
    const observer = await fixture.createDb();
    const snapshots: { id: string; title: string; done: boolean }[][] = [];
    const open = app.todos.where({ done: false }).orderBy("title");
    const unsubscribe = observer.subscribe(open, (rows) => snapshots.push(rows));
    const created = await writer
      .insert(app.todos, { title: "first", done: false })
      .wait({ tier: "local" });
    await expect.poll(async () => writer.all(open, { tier: "local" })).toEqual([created]);
    await expect.poll(async () => observer.all(open, { tier: "local" })).toEqual([created]);
    await expect.poll(() => snapshots.at(-1)).toEqual([created]);
    await writer.update(app.todos, created.id, { title: "updated" }).wait({ tier: "local" });
    await expect
      .poll(async () => observer.one(app.todos.where({ id: created.id }), { tier: "local" }))
      .toMatchObject({ title: "updated" });
    await expect.poll(() => snapshots.at(-1)).toEqual([{ ...created, title: "updated" }]);
    await writer.delete(app.todos, created.id).wait({ tier: "local" });
    await expect.poll(async () => observer.all(open, { tier: "local" })).toEqual([]);
    await expect.poll(() => snapshots.at(-1)).toEqual([]);
    const survivor = await writer
      .insert(app.todos, { title: "survives sibling close", done: false })
      .wait({ tier: "local" });
    await expect.poll(async () => observer.all(open, { tier: "local" })).toEqual([survivor]);
    await expect.poll(() => snapshots.at(-1)).toEqual([survivor]);
    // Keep independent delivery interest while retiring the original stream.
    // A local-only snapshot after the sole subscription closes may stay stale.
    let deliveryMarker: { id: string; title: string; done: boolean }[] = [];
    const stopMarker = observer.subscribe(app.todos.where({ id: survivor.id }), (rows) => {
      deliveryMarker = rows;
    });
    await expect.poll(() => deliveryMarker).toEqual([survivor]);
    unsubscribe();
    const cancelledSnapshotCount = snapshots.length;
    await writer
      .update(app.todos, survivor.id, { title: "after cancellation" })
      .wait({ tier: "local" });
    const afterCancellation = { ...survivor, title: "after cancellation" };
    await expect.poll(() => deliveryMarker).toEqual([afterCancellation]);
    await expect
      .poll(async () => observer.all(open, { tier: "local" }))
      .toEqual([afterCancellation]);
    expect(snapshots).toHaveLength(cancelledSnapshotCount);
    stopMarker();
    await Promise.all([writer.shutdown(), writer.shutdown()]);
    // Shared NativeRuntimeAdapter semantics return no rows after shutdown.
    expect(await writer.all(open, { tier: "local" })).toEqual([]);
    expect(await observer.all(open, { tier: "local" })).toEqual([afterCancellation]);
  });
});
