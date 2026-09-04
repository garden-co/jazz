import { describe, expect, it } from "vitest";
import { ReadTier } from "../../src/runtime/client.js";
import { schema } from "../../src/index.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});

// Browser donor: db.transaction-reads.test.ts, snapshot anchoring, staged
// insert/update/delete isolation, rollback, and mergeable read-own-writes.
describe("React Native transaction reads through the native C ABI", () => {
  for (const kind of ["mergeable", "exclusive"] as const) {
    it(`${kind} isolates simultaneous staged writes and restores the base after rollback`, async () => {
      await withNativeRelayFixture(app, async (fixture) => {
        const db = await fixture.createDb();
        const base = db.insert(app.todos, { title: "base", done: false }).value;
        await expect(db.all(app.todos)).resolves.toEqual([base]);
        const first = kind === "exclusive" ? db.beginExclusiveTransaction() : db.beginTransaction();
        const second =
          kind === "exclusive" ? db.beginExclusiveTransaction() : db.beginTransaction();
        first.update(app.todos, base.id, { title: "first draft" });
        second.update(app.todos, base.id, { title: "second draft" });
        const draft = first.insert(app.todos, { title: "inserted draft", done: true });
        expect(await first.all(app.todos.orderBy("title"))).toEqual([
          { ...base, title: "first draft" },
          draft,
        ]);
        expect(await second.one(app.todos)).toEqual({ ...base, title: "second draft" });
        expect(await db.one(app.todos)).toEqual(base);
        first.delete(app.todos, base.id);
        expect(await first.all(app.todos)).toEqual([draft]);
        await first.rollback();
        await second.rollback();
        expect(await db.all(app.todos)).toEqual([base]);
        await expect(first.all(app.todos)).rejects.toThrow();
      });
    });
  }

  it("anchors an exclusive snapshot at begin while ordinary reads advance", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const before = db.insert(app.todos, { title: "before", done: false }).value;
      await db.all(app.todos);
      const tx = db.beginExclusiveTransaction();
      db.insert(app.todos, { title: "after", done: false });
      expect(await tx.all(app.todos)).toEqual([before]);
      expect(await db.all(app.todos)).toHaveLength(2);
      await tx.rollback();
    });
  });

  it("publishes a mergeable staged update only after commit", async () => {
    await withNativeRelayFixture(app, async (fixture) => {
      const db = await fixture.createDb();
      const base = db.insert(app.todos, { title: "before", done: false }).value;
      await db.all(app.todos);
      const tx = db.beginTransaction();
      tx.update(app.todos, base.id, { title: "committed" });
      expect(await tx.one(app.todos)).toEqual({ ...base, title: "committed" });
      expect(await db.one(app.todos)).toEqual(base);
      await tx.commit();
      expect(await db.one(app.todos)).toEqual({ ...base, title: "committed" });
      await expect(tx.all(app.todos)).rejects.toThrow();
    });
  });
});

it("waits for native upstream authority before accepting an exclusive commit", async () => {
  const { startLocalJazzServer, startTestJwtIssuer, deploy, mergePermissionsIntoWasmSchema } =
    await import("../../src/testing/index.js");
  const issuer = await startTestJwtIssuer();
  const server = await startLocalJazzServer({
    inMemory: true,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
  try {
    const permissions = schema.definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.always(),
      policy.todos.allowInsert.always(),
      policy.todos.allowUpdate.always(),
    ]);
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    await withNativeRelayFixture(
      { wasmSchema: mergePermissionsIntoWasmSchema(app.wasmSchema, permissions) },
      async (fixture) => {
        const db = await fixture.createDb();
        const base = await db
          .insert(app.todos, { title: "before", done: false })
          .wait({ tier: "global" });
        await db.all(app.todos, { tier: ReadTier.Remote });
        await db.disconnect();
        const tx = db.beginExclusiveTransaction();
        tx.update(app.todos, base.id, { title: "accepted" });
        expect(await tx.one(app.todos)).toEqual({ ...base, title: "accepted" });
        const committed = tx.commit();
        let settled = false;
        const accepted = committed.wait();
        void accepted.then(
          () => {
            settled = true;
          },
          () => {
            settled = true;
          },
        );
        // A configured native authority still owns acceptance while offline.
        await new Promise((resolve) => setTimeout(resolve, 100));
        expect(settled).toBe(false);
        await db.reconnect();
        await accepted;
        expect(await db.one(app.todos, { tier: ReadTier.Remote })).toEqual({
          ...base,
          title: "accepted",
        });
      },
      {
        appId: server.appId,
        session: {
          issuer: issuer.issuer,
          user_id: "rn-exclusive-writer",
          claims: { role: "user" },
          authMode: "external",
        },
        upstream: { serverUrl: server.url, jwt: issuer.jwtForUser("rn-exclusive-writer") },
      },
    );
  } finally {
    await server.stop();
    await issuer.stop();
  }
}, 30_000);
