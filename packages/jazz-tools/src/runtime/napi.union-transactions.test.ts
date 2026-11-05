import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createJazzContext } from "../backend/create-jazz-context.js";

const app = s.defineApp({
  todos: s.table({ title: s.string(), done: s.boolean() }, {}),
  checks: s.table({ title: s.string() }, {}),
});

it("reads duplicate union occurrences in a serializable transaction and rejects a stale page", async () => {
  const root = await mkdtemp(join(tmpdir(), "jazz-union-transaction-"));
  const context = createJazzContext({
    appId: randomUUID(),
    app,
    permissions: {},
    driver: { type: "persistent", dataPath: join(root, "runtime.db") },
  });
  try {
    const db = context.db();
    const alpha = await db
      .insert(app.todos, { title: "alpha", done: false })
      .wait({ tier: "local" });
    await db.insert(app.todos, { title: "beta", done: false }).wait({ tier: "local" });
    const query = app
      .union({
        byDone: app.todos.where({ done: false }),
        byTitle: app.todos.where({ title: "alpha" }),
      })
      .orderBy("title")
      .limit(2);
    const first = db.beginSerializableTransaction();
    await expect(first.all(query, { tier: "local-first" })).resolves.toMatchObject([
      { id: alpha.id },
      { id: alpha.id },
    ]);
    first.insert(app.checks, { title: "accepted" });
    await first.commit().wait({ tier: "local" });
    const stale = db.beginSerializableTransaction();
    await expect(stale.all(query, { tier: "local-first" })).resolves.toMatchObject([
      { id: alpha.id },
      { id: alpha.id },
    ]);
    stale.insert(app.checks, { title: "must not survive" });
    await db.update(app.todos, alpha.id, { done: true }).wait({ tier: "local" });
    // The transaction still observes its pinned snapshot.
    await expect(stale.all(query, { tier: "local-first" })).resolves.toMatchObject([
      { id: alpha.id },
      { id: alpha.id },
    ]);
    await expect((async () => stale.commit().wait({ tier: "local" }))()).rejects.toMatchObject({
      code: "transaction_conflict",
    });
    await expect(db.all(app.checks.where({}), { tier: "local-first" })).resolves.toMatchObject([
      { title: "accepted" },
    ]);
  } finally {
    await context.shutdown();
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);
