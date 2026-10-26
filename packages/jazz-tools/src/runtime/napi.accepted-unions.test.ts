import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import { deploy } from "../dev/catalogue.js";
import { startLocalJazzServer } from "../testing/index.js";
import { prepareDbTransaction, type Db, type QueryBuilder } from "./db.js";

const app = s.defineApp({
  rooms: s.table({ title: s.string(), code: s.string() }, {}),
  links: s.table({ room: s.uuid(), code: s.string() }, { roomRelation: s.rel("rooms", "room") }),
  checks: s.table({ title: s.string() }, {}),
});
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.rooms.allowRead.where({ code: session.claims["code"] });
  policy.rooms.allowInsert.always();
  policy.rooms.allowUpdate.always();
  policy.links.allowRead.where({ code: session.claims["code"] });
  policy.links.allowInsert.always();
  policy.links.allowDelete.always();
  policy.checks.allowRead.always();
  policy.checks.allowInsert.always();
});

async function fixture() {
  const appId = randomUUID();
  const backendSecret = `accepted-union-backend-${appId}`;
  const adminSecret = `accepted-union-admin-${appId}`;
  const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
  const contexts: ReturnType<typeof createJazzContext>[] = [];
  const close = async () => {
    for (const context of contexts.reverse()) await context.shutdown();
    await server.stop();
  };
  try {
    await deploy({ appId, serverUrl: server.url, adminSecret, schema: app, permissions });
    const open = () => {
      const context = createJazzContext({
        appId,
        app,
        permissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
      });
      contexts.push(context);
      return context;
    };
    const writer = open().asBackend(app);
    const ids = new Map<string, string>();
    const transactionIds = new Map<string, string>();
    for (const [title, code] of [
      ["aardvark", "a"],
      ["alpha", "a"],
      ["beta", "b"],
      ["gamma", "a"],
    ] as const) {
      const write = writer.insert(app.rooms, { title, code });
      const room = await write.wait({ tier: "global" });
      ids.set(title, room.id);
      transactionIds.set(title, await write.txId);
    }
    const links = new Map<string, string>();
    for (const [title, code] of [
      ["alpha", "a"],
      ["beta", "b"],
    ] as const) {
      const link = await writer
        .insert(app.links, { room: ids.get(title)!, code })
        .wait({ tier: "global" });
      links.set(title, link.id);
    }
    const session = (code: string, context = open()) =>
      context.forSession(
        {
          issuer: "accepted-union-tests",
          user_id: "alice",
          account_id: localFirstAccountId("alice", "accepted-union-tests"),
          authMode: "external",
          claims: { code },
        },
        app,
      );
    return { writer, open, session, ids, transactionIds, links, close };
  } catch (error) {
    await close();
    throw error;
  }
}
function query(narrowed: boolean) {
  return app
    .union({
      all: narrowed ? app.rooms.select("title") : app.rooms,
      linked: narrowed
        ? app.links.hopTo("roomRelation").select("title")
        : app.links.hopTo("roomRelation"),
    })
    .orderBy("title");
}
async function read<T extends { id: string }>(db: Db, query: QueryBuilder<T>) {
  return (await db.serializableTransaction((tx) => tx.allSettledForE2ee(query))).wait({
    tier: "global",
  });
}

// A successful or authority-rejected commit has already closed its local
// transaction. Cleanup still rolls back any transaction left open by a failed assertion.
async function rollbackIfOpen(tx: { rollback(): Promise<boolean> }) {
  try {
    await tx.rollback();
  } catch (error) {
    if (
      !(error instanceof Error) ||
      !/transaction .*is (?:already committed|not open)/.test(error.message)
    )
      throw error;
  }
}

// Alice's cold pages preserve duplicate root occurrences and original content
// receipts, with independent immutable claims on the same backend connection.
it.each([false, true])(
  "pairs accepted UNION occurrences through NAPI (narrowed: %s)",
  async (narrowed) => {
    const f = await fixture();
    try {
      const shared = f.open();
      const a = f.session("a", shared);
      const b = f.session("b", shared);
      const nobody = f.session("nobody", shared);
      const all = query(narrowed);
      const page = all.offset(1).limit(2);
      const [alpha, beta, empty] = await Promise.all([
        read(a, page),
        read(b, all),
        read(nobody, all),
      ]);
      for (const [snapshot, title] of [
        [alpha, "alpha"],
        [beta, "beta"],
      ] as const) {
        expect(snapshot.rows.map(({ id, title }) => ({ id, title }))).toEqual([
          { id: f.ids.get(title), title },
          { id: f.ids.get(title), title },
        ]);
        expect(snapshot.settlements).toHaveLength(2);
        expect(snapshot.settlements[0]).toEqual(snapshot.settlements[1]);
        expect(snapshot.settlements[0]).toEqual({
          rowId: f.ids.get(title),
          transactionId: f.transactionIds.get(title),
          position: expect.stringMatching(/^\d+$/),
        });
        if (narrowed) for (const row of snapshot.rows) expect(row).not.toHaveProperty("code");
      }
      expect(empty).toEqual({ rows: [], settlements: [] });
      expect(await read(f.open().asBackend(app), page)).toEqual(alpha);
      const manual = f.session("a").beginSerializableTransaction();
      try {
        expect(await manual.allSettledForE2ee(page)).toEqual(alpha);
        expect(await manual.allSettledForE2ee(page)).toEqual(alpha);
        await manual.commit().wait({ tier: "global" });
      } finally {
        await rollbackIfOpen(manual);
      }
      // E2EE preparation has its own scoped transaction entry point.
      const prepared = f.session("a").beginSerializableTransaction();
      try {
        await prepareDbTransaction(prepared, async (scope) => {
          expect(await scope.allSettledForE2ee(page)).toEqual(alpha);
        });
        await prepared.commit().wait({ tier: "global" });
      } finally {
        await rollbackIfOpen(prepared);
      }
    } finally {
      await f.close();
    }
  },
  30_000,
);

// Alice's trusted backend validates the original query under staged writes.
// An unrelated page successor remains private until commit; a predecessor
// changes the page and must poison the transaction instead of hiding the row.
it("keeps accepted UNION staged-page checks rollback-only through the SDK", async () => {
  const f = await fixture();
  try {
    const page = query(true).offset(1).limit(2);
    const first = f.session("a").beginSerializableTransaction();
    try {
      first.insert(app.rooms, { title: "zzzz", code: "a" });
      const snapshot = await first.allSettledForE2ee(page);
      expect(snapshot.rows.map((row) => row.title)).toEqual(["alpha", "alpha"]);
      expect(await f.writer.all(app.rooms.where({ title: "zzzz" }), { tier: "remote" })).toEqual(
        [],
      );
      await first.commit().wait({ tier: "global" });
      expect(
        await f.writer.all(app.rooms.where({ title: "zzzz" }), { tier: "remote" }),
      ).toHaveLength(1);
    } finally {
      await rollbackIfOpen(first);
    }
    const phantom = f.session("a").beginSerializableTransaction();
    try {
      phantom.insert(app.rooms, { title: "aa", code: "a" });
      await expect(phantom.allSettledForE2ee(page)).rejects.toMatchObject({ code: "not_observed" });
      await expect((async () => phantom.commit().wait({ tier: "global" }))()).rejects.toThrow();
      expect(await f.writer.all(app.rooms.where({ title: "aa" }), { tier: "remote" })).toEqual([]);
    } finally {
      await rollbackIfOpen(phantom);
    }
  } finally {
    await f.close();
  }
}, 30_000);

// Bob removes Alice's joined occurrence without changing root content. The
// original full query must still conflict at Global commit and roll back its
// companion write. A fresh empty accepted read must likewise retain phantoms.
it("validates original accepted UNION witnesses and empty results at Global commit", async () => {
  const f = await fixture();
  try {
    const tx = f.session("a").beginSerializableTransaction();
    try {
      const snapshot = await tx.allSettledForE2ee(query(true).offset(1).limit(2));
      expect(snapshot.rows.map((row) => row.title)).toEqual(["alpha", "alpha"]);
      tx.insert(app.checks, { title: "must not publish" });
      await f.writer.delete(app.links, f.links.get("alpha")!).wait({ tier: "global" });
      await expect((async () => tx.commit().wait({ tier: "global" }))()).rejects.toMatchObject({
        code: "transaction_conflict",
      });
      expect(await f.writer.all(app.checks, { tier: "remote" })).toEqual([]);
    } finally {
      await rollbackIfOpen(tx);
    }
    const empty = f.session("nobody").beginSerializableTransaction();
    try {
      expect(await empty.allSettledForE2ee(query(false))).toEqual({ rows: [], settlements: [] });
      await f.writer
        .insert(app.rooms, { title: "newly visible", code: "nobody" })
        .wait({ tier: "global" });
      await expect((async () => empty.commit().wait({ tier: "global" }))()).rejects.toMatchObject({
        code: "transaction_conflict",
      });
    } finally {
      await rollbackIfOpen(empty);
    }
  } finally {
    await f.close();
  }
}, 30_000);
