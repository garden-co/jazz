import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createJazz, memory, sqlite, remote, schema as s, type JazzCoreProvider } from "./index.js";
import { startLocalJazzServer } from "../dev/dev-server.js";

const app = s.defineApp(
  { notes: s.table({ title: s.string(), owner: s.uuid() }, {}) },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.notes.allowRead.where({ owner: session.user });
  policy.notes.allowInsert.where({ owner: session.user });
  policy.notes.allowUpdate.where({ owner: session.user });
  policy.notes.allowDelete.where({ owner: session.user });
});
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanup.length) await cleanup.pop()!();
});
const request = (user: string) =>
  new Request("http://app/test", { headers: { "x-test-user": user } });
async function open(kind: string) {
  let store: JazzCoreProvider;
  if (kind === "memory") store = memory();
  else if (kind === "sqlite") {
    const path = await mkdtemp(join(tmpdir(), "jazz-backend-tx-"));
    cleanup.push(() => rm(path, { recursive: true, force: true }));
    store = sqlite({ path });
  } else {
    const core = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });
    cleanup.push(() => core.stop());
    store = remote({ url: core.url, appId: core.appId, secret: core.adminSecret });
  }
  const jazz = createJazz({
    app,
    permissions,
    store,
    authenticate: (req) => ({ userId: req.headers.get("x-test-user")! }),
  });
  cleanup.push(() => jazz.close());
  await jazz.ready();
  return jazz;
}

describe.each(["memory", "sqlite", "remote"])("createJazz %s backend transactions", (provider) => {
  it.each(["mergeable", "serializable"] as const)(
    "commits and rolls back %s writes with private reads",
    async (kind) => {
      const jazz = await open(provider);
      const owner = randomUUID();
      const committed = await (
        kind === "mergeable"
          ? jazz.db.transaction.bind(jazz.db)
          : jazz.db.serializableTransaction.bind(jazz.db)
      )(async (tx) => {
        tx.insert(app.notes, { title: "first", owner });
        await Promise.resolve();
        tx.insert(app.notes, { title: "second", owner });
        expect((await tx.all(app.notes)).map((row) => row.title).sort()).toEqual([
          "first",
          "second",
        ]);
        expect(await jazz.db.all(app.notes)).toEqual([]);
        return "committed";
      });
      await committed.wait({ tier: "global" });
      expect(committed.value).toBe("committed");
      await expect(
        jazz.db.transaction(async (tx) => {
          tx.insert(app.notes, { title: "abandoned", owner });
          await tx.all(app.notes);
          throw new Error("abort callback");
        }),
      ).rejects.toThrow("abort callback");
      expect((await jazz.db.all(app.notes)).map((row) => row.title).sort()).toEqual([
        "first",
        "second",
      ]);
    },
  );

  it("retains user permissions and backend attribution through commit", async () => {
    const jazz = await open(provider);
    const alice = await jazz.createAccountFor("alice"),
      bob = await jazz.createAccountFor("bob");
    const asAlice = await jazz.forReqUser(request("alice"));
    const asBob = await jazz.forReqUser(request("bob"));
    await (
      await asAlice.serializableTransaction(async (tx) => {
        tx.insert(app.notes, { title: "alice", owner: alice });
        expect((await tx.all(app.notes)).map((row) => row.title)).toEqual(["alice"]);
      })
    ).wait({ tier: "global" });
    expect(await asBob.all(app.notes)).toEqual([]);
    const rejected = asAlice
      .transaction((tx) => {
        tx.insert(app.notes, { title: "must roll back", owner: alice });
        tx.insert(app.notes, { title: "forbidden", owner: bob });
      })
      .then((receipt) => receipt.wait({ tier: "global" }));
    await expect(rejected).rejects.toThrow();
    expect((await jazz.db.all(app.notes)).map((row) => row.title)).toEqual(["alice"]);

    const attributed = await jazz.withAttribution(request("alice"));
    for (const scoped of [asAlice, attributed]) {
      await expect(
        scoped.transaction(async (tx) => {
          tx.insert(app.notes, { title: "abandoned scope", owner: alice });
          await tx.all(app.notes);
          throw new Error("abort scoped callback");
        }),
      ).rejects.toThrow("abort scoped callback");
    }
    expect((await jazz.db.all(app.notes)).map((row) => row.title)).toEqual(["alice"]);

    await (
      await attributed.serializableTransaction(async (tx) => {
        // Alice's normal permissions prohibit this; attribution retains backend authority.
        tx.insert(app.notes, { title: "attributed", owner: bob });
        expect((await tx.all(app.notes)).map((row) => row.title).sort()).toEqual([
          "alice",
          "attributed",
        ]);
      })
    ).wait({ tier: "global" });
    const row = await jazz.db.one(app.notes.where({ title: "attributed" }).select("$createdBy"));
    expect(row?.$createdBy).toMatchObject({ account: alice });
  });

  it("rejects serializable conflicts without overwriting the intervening write", async () => {
    const jazz = await open(provider);
    const row = await jazz.db
      .insert(app.notes, { title: "initial", owner: randomUUID() })
      .wait({ tier: "global" });
    const tx = jazz.db.beginSerializableTransaction();
    expect((await tx.all(app.notes))[0]?.title).toBe("initial");
    await jazz.db.update(app.notes, row.id, { title: "intervening" }).wait({ tier: "global" });
    tx.update(app.notes, row.id, { title: "conflicting" });
    await expect(tx.commit().wait({ tier: "global" })).rejects.toThrow();
    expect((await jazz.db.one(app.notes.where({ id: row.id })))?.title).toBe("intervening");
  });
});
