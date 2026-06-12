import { defineSuite } from "../runner/harness";
import { app } from "../schema";

type Delta = { all: Array<{ id: string }>; delta: Array<{ kind: number; id: string }> };

export default defineSuite("subscriptions", ({ test }) => {
  test("emits add and update deltas", async ({ db, expect, waitForCondition }) => {
    const deltas: Delta[] = [];
    const unsubscribe = db.subscribeAll(app.todos, (d) => deltas.push(d as unknown as Delta));
    try {
      const { value: created } = db.insert(app.todos, { title: "watch me", done: false });

      await waitForCondition(
        () => deltas.some((d) => d.delta.some((c) => c.kind === 0 && c.id === created.id)),
        5000,
        "add delta",
      );

      db.update(app.todos, created.id, { done: true });
      await waitForCondition(
        () => deltas.some((d) => d.delta.some((c) => c.kind === 2 && c.id === created.id)),
        5000,
        "update delta",
      );

      const latest = deltas[deltas.length - 1]!;
      expect(latest.all.some((t) => t.id === created.id)).toBe(true);
    } finally {
      unsubscribe();
    }
  });
});
